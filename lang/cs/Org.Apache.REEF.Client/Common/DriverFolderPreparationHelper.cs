// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System.Collections.Generic;
using System.IO;
using System.Linq;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Common.Files;
using Org.Apache.REEF.Common.Jar;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
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
        private const string DefaultDriverConfigurationFileContents =
        @"<configuration>" +
        @"  <runtime>" +
        @"    <assemblyBinding xmlns=""urn:schemas-microsoft-com:asm.v1"">" +
        @"      <dependentAssembly>" +
        @"        <assemblyIdentity name=""Newtonsoft.Json"" publicKeyToken=""30ad4fe6b2a6aeed"" culture=""neutral"" />" +
        @"        <bindingRedirect oldVersion=""0.0.0.0-10.0.0.0"" newVersion=""10.0.0.0"" />" +
        @"      </dependentAssembly>" +
        @"      <probing privatePath=""local;global""/>" +
        @"    </assemblyBinding>" +
        @"  </runtime>" +
        @"</configuration>";
        private const string EvaluatorExecutableConfig = "Org.Apache.REEF.Evaluator.exe.config";

        private static readonly Logger Logger = Logger.GetLogger(typeof(DriverFolderPreparationHelper));
        private readonly AvroConfigurationSerializer _configurationSerializer;
        private readonly REEFFileNames _fileNames;
        private readonly FileSets _fileSets;
        private readonly ISet<IConfigurationProvider> _driverConfigurationProviders;

        /// <summary>
        /// The folder in which we search for the client jar.
        /// In the manner of JavaClientLauncher.cs.
        /// </summary>
        private const string JarFolder = "./";

        [Inject]
        internal DriverFolderPreparationHelper(
            REEFFileNames fileNames,
            AvroConfigurationSerializer configurationSerializer,
            FileSets fileSets,
            [Parameter(typeof(EnvironmentDriverConfigurationProviders))] ISet<IConfigurationProvider> driverConfigurationProviders)
        {
            _fileNames = fileNames;
            _configurationSerializer = configurationSerializer;
            _fileSets = fileSets;
            _driverConfigurationProviders = driverConfigurationProviders;
        }

        /// <summary>
        /// Prepares the working directory for a Driver in driverFolderPath.
        /// </summary>
        /// <param name="appParameters"></param>
        /// <param name="driverFolderPath"></param>
        internal void PrepareDriverFolder(AppParameters appParameters, string driverFolderPath)
        {
            Logger.Log(Level.Verbose, "Preparing Driver filesystem layout in {0}", driverFolderPath);

            // Setup the folder structure
            CreateDefaultFolderStructure(appParameters, driverFolderPath);

            // Add the appParameters into that folder structure
            _fileSets.AddJobFiles(appParameters);

            // Add the reef-bridge-client jar to the global files in the manner of JavaClientLauncher.cs.
            _fileSets.AddToLocalFiles(Directory.GetFiles(JarFolder)
                .Where(file => !string.IsNullOrWhiteSpace(file))
                .Where(jarFile => Path.GetFileName(jarFile).ToLower().StartsWith(ClientConstants.ClientJarFilePrefix)));

            // Create the driver configuration
            CreateDriverConfiguration(appParameters, driverFolderPath);

            // Initiate the final copy
            _fileSets.CopyToDriverFolder(driverFolderPath);

            Logger.Log(Level.Info, "Done preparing Driver filesystem layout in {0}", driverFolderPath);
        }

        /// <summary>
        /// Merges the Configurations in appParameters and serializes them into the right place within driverFolderPath,
        /// assuming
        /// that points to a Driver's working directory.
        /// </summary>
        /// <param name="appParameters"></param>
        /// <param name="driverFolderPath"></param>
        internal void CreateDriverConfiguration(AppParameters appParameters, string driverFolderPath)
        {
            var driverConfigurations = _driverConfigurationProviders.Select(configurationProvider => configurationProvider.GetConfiguration()).ToList();
            var driverConfiguration = Configurations.Merge(driverConfigurations.Concat(appParameters.DriverConfigurations).ToArray());

            _configurationSerializer.ToFile(driverConfiguration,
                Path.Combine(driverFolderPath, _fileNames.GetClrDriverConfigurationPath()));
        }

        /// <summary>
        /// Creates the driver folder structure in this given folder as the root
        /// </summary>
        /// <param name="appParameters">Job submission information</param>
        /// <param name="driverFolderPath">Driver folder path</param>
        internal void CreateDefaultFolderStructure(AppParameters appParameters, string driverFolderPath)
        {
            Directory.CreateDirectory(Path.Combine(driverFolderPath, _fileNames.GetReefFolderName()));
            Directory.CreateDirectory(Path.Combine(driverFolderPath, _fileNames.GetLocalFolderPath()));
            Directory.CreateDirectory(Path.Combine(driverFolderPath, _fileNames.GetGlobalFolderPath()));

            var resourceHelper = new ResourceHelper(typeof(DriverFolderPreparationHelper).Assembly);
            foreach (var fileResources in ResourceHelper.FileResources)
            {
                var fileName = resourceHelper.GetString(fileResources.Key);
                if (ResourceHelper.ClrDriverFullName == fileResources.Key)
                {
                    fileName = Path.Combine(driverFolderPath, _fileNames.GetBridgeExePath());
                }
                if (!File.Exists(fileName))
                {
                    File.WriteAllBytes(fileName, resourceHelper.GetBytes(fileResources.Value));
                }
            }
            
            // generate .config file for bridge executable
            var config = DefaultDriverConfigurationFileContents;
            if (!string.IsNullOrEmpty(appParameters.DriverConfigurationFileContents))
            {
                config = appParameters.DriverConfigurationFileContents;
            }
            File.WriteAllText(Path.Combine(driverFolderPath, _fileNames.GetBridgeExeConfigPath()), config);

            // generate .config file for Evaluator executable
            var userDefinedEvaluatorConfigFileName = Path.Combine(JarFolder, EvaluatorExecutableConfig);
            var evaluatorConfigFilName = Path.Combine(driverFolderPath, _fileNames.GetGlobalFolderPath(), EvaluatorExecutableConfig);
            string evaluatorAppConfigString = DefaultDriverConfigurationFileContents;

            if (File.Exists(userDefinedEvaluatorConfigFileName))
            {
                evaluatorAppConfigString = File.ReadAllText(userDefinedEvaluatorConfigFileName);
            }
            Logger.Log(Level.Verbose, "Create EvaluatorConfigFile {0} with config {1}.", evaluatorConfigFilName, evaluatorAppConfigString);
            File.WriteAllText(evaluatorConfigFilName, evaluatorAppConfigString);
        }
    }
}
