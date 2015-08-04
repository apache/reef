/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


using System;
using System.IO;
using System.Linq;
using System.Resources;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Common;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.Common
{
    /// <summary>
    /// Helps prepare the driver folder.
    /// </summary>
    internal sealed class DriverFolderPreparationHelper
    {
        private const string DLLFileNameExtension = ".dll";
        private const string EXEFileNameExtension = ".exe";
        private const string ClientJarResourceName = "reef_bridge_client";
        private const string ClientJarFileNameResourceName = "ClientJarFullName";
        private const string DriverJarResourceName = "reef_bridge_driver";
        private const string DriveJarFileNameResourceName = "DriverJarFullName";
        private const string CouldNotRetrieveResource = "Could not retrieve resource '{0}'";

        private static readonly Logger Logger = Logger.GetLogger(typeof(DriverFolderPreparationHelper));
        private readonly AvroConfigurationSerializer _configurationSerializer;
        private readonly REEFFileNames _fileNames;
        private readonly FileSets _fileSets;

        [Inject]
        internal DriverFolderPreparationHelper(
            REEFFileNames fileNames,
            AvroConfigurationSerializer configurationSerializer,
            FileSets fileSets)
        {
            _fileNames = fileNames;
            _configurationSerializer = configurationSerializer;
            _fileSets = fileSets;
        }

        /// <summary>
        /// Prepares the working directory for a Driver in driverFolderPath.
        /// </summary>
        /// <param name="jobSubmission"></param>
        /// <param name="driverFolderPath"></param>
        internal void PrepareDriverFolder(IJobSubmission jobSubmission, string driverFolderPath)
        {
            Logger.Log(Level.Info, "Preparing Driver filesystem layout in " + driverFolderPath);

            // Setup the folder structure
            CreateDefaultFolderStructure(driverFolderPath);

            // Add the jobSubmission into that folder structure
            _fileSets.AddJobFiles(jobSubmission);

            // Create the driver configuration
            CreateDriverConfiguration(jobSubmission, driverFolderPath);

            // Add the REEF assemblies
            AddAssemblies();

            // Initiate the final copy
            _fileSets.CopyToDriverFolder(driverFolderPath);

            Logger.Log(Level.Info, "Done preparing Driver filesystem layout in " + driverFolderPath);
        }

        /// <summary>
        /// Merges the Configurations in jobSubmission and serializes them into the right place within driverFolderPath,
        /// assuming
        /// that points to a Driver's working directory.
        /// </summary>
        /// <param name="jobSubmission"></param>
        /// <param name="driverFolderPath"></param>
        internal void CreateDriverConfiguration(IJobSubmission jobSubmission, string driverFolderPath)
        {
            var driverConfiguration = Configurations.Merge(jobSubmission.DriverConfigurations.ToArray());

            _configurationSerializer.ToFile(driverConfiguration,
                Path.Combine(driverFolderPath, _fileNames.GetClrDriverConfigurationPath()));

            // TODO: Remove once we cleaned up the Evaluator to not expect this [REEF-216]
            _configurationSerializer.ToFile(driverConfiguration,
                Path.Combine(driverFolderPath, _fileNames.GetGlobalFolderPath(), Constants.ClrBridgeRuntimeConfiguration));
        }

        /// <summary>
        /// Creates the driver folder structure in this given folder as the root
        /// </summary>
        /// <param name="driverFolderPath"></param>
        internal void CreateDefaultFolderStructure(string driverFolderPath)
        {
            Directory.CreateDirectory(Path.Combine(driverFolderPath, _fileNames.GetReefFolderName()));
            Directory.CreateDirectory(Path.Combine(driverFolderPath, _fileNames.GetLocalFolderPath()));
            Directory.CreateDirectory(Path.Combine(driverFolderPath, _fileNames.GetGlobalFolderPath()));
        }

        /// <summary>
        /// Adds all Assemlies to the Global folder in the Driver.
        /// </summary>
        private void AddAssemblies()
        {
            var assembly = typeof(DriverFolderPreparationHelper).Assembly;
            var names = assembly.GetManifestResourceNames();
            if (null == names[0] )
            {
                Exceptions.Throw(new ApplicationException("Could not retrieve Assembly Manifest Resource names"), Logger);
            }
            var manifestResources = assembly.GetManifestResourceStream(names[0]);
            if (null == manifestResources)
            {
                Exceptions.Throw(new ApplicationException("Could not retrieve Assembly Manifest Resource stream"), Logger);
            }

            var resourceSet = new ResourceSet(manifestResources);

            var clientJarBytes = resourceSet.GetObject(ClientJarResourceName) as byte[];
            if (null == clientJarBytes)
            {
                throw new ApplicationException(string.Format(CouldNotRetrieveResource, ClientJarResourceName));
            }
            var clientJarFileName = resourceSet.GetObject(ClientJarFileNameResourceName) as string;
            if (null == clientJarFileName)
            {
                throw new ApplicationException(string.Format(CouldNotRetrieveResource, ClientJarFileNameResourceName));
            }
            var driverJarBytes = resourceSet.GetObject(DriverJarResourceName) as byte[];
            if (null == driverJarBytes)
            {
                throw new ApplicationException(string.Format(CouldNotRetrieveResource, DriverJarResourceName));
            }
            var driverJarFileName = resourceSet.GetObject(DriveJarFileNameResourceName) as string;
            if (null == driverJarFileName)
            {
                throw new ApplicationException(string.Format(CouldNotRetrieveResource, DriveJarFileNameResourceName));
            }

            File.WriteAllBytes(clientJarFileName, clientJarBytes);
            File.WriteAllBytes(driverJarFileName, driverJarBytes);

            // TODO: Be more precise, e.g. copy the JAR only to the driver.
            var assemblies = Directory.GetFiles(@".\").Where(IsAssemblyToCopy);
            _fileSets.AddToGlobalFiles(assemblies);
        }

        /// <summary>
        /// Returns true, if the given file path references a DLL or EXE or JAR.
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        private static Boolean IsAssemblyToCopy(string filePath)

        {
            var fileName = Path.GetFileName(filePath);
            if (string.IsNullOrWhiteSpace(fileName))
            {
                return false;
            }
            var lowerCasePath = fileName.ToLower();
            return lowerCasePath.EndsWith(DLLFileNameExtension) ||
                   lowerCasePath.EndsWith(EXEFileNameExtension) ||
                   lowerCasePath.StartsWith(ClientConstants.DriverJarFilePrefix);
        }
    }
}