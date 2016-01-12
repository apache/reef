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
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Common;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
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
        private const string BridgeExe = "Org.Apache.REEF.Bridge.exe";
        private const string ClrDriverFullName = "ClrDriverFullName";
        private const string DefaultDriverConfigurationFileContents =
        @"<configuration>" +
        @"  <runtime>" +
        @"    <assemblyBinding xmlns=""urn:schemas-microsoft-com:asm.v1"">" +
        @"      <probing privatePath=""local;global""/>" +
        @"    </assemblyBinding>" +
        @"  </runtime>" +
        @"</configuration>";

        // We embed certain binaries in client dll.
        // Following items in tuples refer to resource names in Org.Apache.REEF.Client.dll
        // The first resource item contains the name of the file 
        // such as "reef-bridge-java-0.13.0-SNAPSHOT-shaded.jar". The second resource
        // item contains the byte contents for said file.
        // Please note that the identifiers below need to be in sync with 2 other files
        // 1. $(SolutionDir)\Org.Apache.REEF.Client\Properties\Resources.xml
        // 2. $(SolutionDir)\Org.Apache.REEF.Client\Org.Apache.REEF.Client.csproj
        private readonly static Tuple<string, string>[] clientFileResources = new Tuple<string, string>[]
        {
            new Tuple<string, string>("ClientJarFullName", "reef_bridge_client"),
            new Tuple<string, string>("DriverJarFullName", "reef_bridge_driver"),
            new Tuple<string, string>(ClrDriverFullName,    "reef_clrdriver"),
        };

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
            CreateDefaultFolderStructure(jobSubmission, driverFolderPath);

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

            // TODO: Remove once we cleaned up the Evaluator to not expect this [REEF-217]
            _configurationSerializer.ToFile(driverConfiguration,
                Path.Combine(driverFolderPath, _fileNames.GetGlobalFolderPath(), _fileNames.GetClrBridgeConfigurationName()));
        }

        /// <summary>
        /// Creates the driver folder structure in this given folder as the root
        /// </summary>
        /// <param name="jobSubmission">Job submission information</param>
        /// <param name="driverFolderPath">Driver folder path</param>
        internal void CreateDefaultFolderStructure(IJobSubmission jobSubmission, string driverFolderPath)
        {
            Directory.CreateDirectory(Path.Combine(driverFolderPath, _fileNames.GetReefFolderName()));
            Directory.CreateDirectory(Path.Combine(driverFolderPath, _fileNames.GetLocalFolderPath()));
            Directory.CreateDirectory(Path.Combine(driverFolderPath, _fileNames.GetGlobalFolderPath()));

            var resourceHelper = new ResourceHelper(typeof(DriverFolderPreparationHelper).Assembly);
            foreach (var fileResources in clientFileResources)
            {
                var fileName = resourceHelper.GetString(fileResources.Item1);
                if (ClrDriverFullName == fileResources.Item1)
                {
                    fileName = Path.Combine(driverFolderPath, _fileNames.GetBridgeExePath());
                }
                File.WriteAllBytes(fileName, resourceHelper.GetBytes(fileResources.Item2));
            }
            
            var config = DefaultDriverConfigurationFileContents;
            if (!string.IsNullOrEmpty(jobSubmission.DriverConfigurationFileContents))
            {
                config = jobSubmission.DriverConfigurationFileContents;
            }
            File.WriteAllText(Path.Combine(driverFolderPath, _fileNames.GetBridgeExeConfigPath()), config);
        }

        /// <summary>
        /// Adds all Assemlies to the Global folder in the Driver.
        /// </summary>
        private void AddAssemblies()
        {
            // TODO: Be more precise, e.g. copy the JAR only to the driver.
            var assemblies = Directory.GetFiles(@".\").Where(IsAssemblyToCopy);
            _fileSets.AddToGlobalFiles(assemblies);
        }

        /// <summary>
        /// Returns true, if the given file path references a DLL or EXE or JAR.
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        private static bool IsAssemblyToCopy(string filePath)
        {
            var fileName = Path.GetFileName(filePath);
            if (string.IsNullOrWhiteSpace(fileName))
            {
                return false;
            }
            var lowerCasePath = fileName.ToLower();
            return lowerCasePath.EndsWith(DLLFileNameExtension) ||
                   lowerCasePath.EndsWith(EXEFileNameExtension) ||
                   lowerCasePath.StartsWith(ClientConstants.ClientJarFilePrefix);
        }
    }
}