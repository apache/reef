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

using System;
using System.Collections.Generic;
using Org.Apache.REEF.Common.Client.Parameters;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;
using System.IO;
using System.Linq;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Client.API
{
    public sealed class JobRequestBuilder
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(JobRequestBuilder));
        private readonly AppParametersBuilder _appParametersBuilder = AppParametersBuilder.NewBuilder();
        private readonly JobParametersBuilder _jobParametersBuilder = JobParametersBuilder.NewBuilder();
        private const string DLLFileNameExtension = ".dll";
        private const string EXEFileNameExtension = ".exe";
        private bool _assembliesAdded = false;

        private JobRequestBuilder()
        {
        }

        [Inject]
        internal JobRequestBuilder(
            [Parameter(typeof(DriverConfigurationProviders))] ISet<IConfigurationProvider> configurationProviders)
        {
            AddDriverConfigurationProviders(configurationProviders);
        }

        /// <summary>
        /// Bake the information provided so far and return a IJobSubmission 
        /// </summary>
        public JobRequest Build()
        {
            // If no assemblies have been added, default to those in the working directory
            if (!_assembliesAdded)
            {
                Logger.Log(Level.Warning, "No assemlies added to the job; Adding assemblies from the current working directory.");
                AddGlobalAssembliesInDirectory(Directory.GetCurrentDirectory());
            }

            return new JobRequest(_jobParametersBuilder.Build(), _appParametersBuilder.Build());
        }

        /// <summary>
        /// Make this file available to all containers
        /// </summary>
        public JobRequestBuilder AddGlobalFile(string fileName)
        {
            ThrowIfFileDoesnotExist(fileName, "Global File");
            _appParametersBuilder.AddGlobalFile(fileName);
            return this;
        }

        /// <summary>
        /// Files specific to one container
        /// </summary>
        public JobRequestBuilder AddLocalFile(string fileName)
        {
            ThrowIfFileDoesnotExist(fileName, "Local File");
            _appParametersBuilder.AddLocalFile(fileName);
            return this;
        }

        /// <summary>
        /// Add an assembly to be made available on all containers.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public JobRequestBuilder AddGlobalAssembly(string fileName)
        {
            ThrowIfFileDoesnotExist(fileName, "Global Assembly");
            _appParametersBuilder.AddGlobalAssembly(fileName);
            _assembliesAdded = true;
            return this;
        }

        /// <summary>
        /// Add all the assemblies in a directory of type EXE, DLL, or with the Client-JarFile Prefix to the global path
        /// </summary>
        /// <param name="globalAssemblyDirectory">The directory to search for assemblies</param>
        public JobRequestBuilder AddGlobalAssembliesInDirectory(string globalAssemblyDirectory)
        {
            if (Directory.Exists(globalAssemblyDirectory))
            {
                // For input paths that are directories, extract only files of a predetermined type
                foreach (var assembly in Directory.GetFiles(globalAssemblyDirectory).Where(IsAssemblyToCopy))
                {
                    AddGlobalAssembly(assembly);
                }
            }
            else
            {
                // Throw if a path input was not a file or a directory
                throw new FileNotFoundException(string.Format("Global Assembly Directory not Found: {0}", globalAssemblyDirectory));
            }
            return this;
        }

        /// <summary>
        /// Add any assemblies of type EXE, DLL, or with the Client-JarFile Prefix to the global path
        /// found in the same directory as the executing assembly's directory
        /// </summary>
        public JobRequestBuilder AddGlobalAssembliesInDirectoryOfExecutingAssembly()
        {
            var directory = ClientUtilities.GetPathToExecutingAssembly();
            return AddGlobalAssembliesInDirectory(directory);
        }

        /// <summary>
        /// Add an assembly to the driver only.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public JobRequestBuilder AddLocalAssembly(string fileName)
        {
            ThrowIfFileDoesnotExist(fileName, "Local Assembly");
            _appParametersBuilder.AddLocalAssembly(fileName);
            _assembliesAdded = true;
            return this;
        }

        /// <summary>
        /// Add a Configuration to the Driver.
        /// </summary>
        /// <param name="configuration"></param>
        /// <returns></returns>
        public JobRequestBuilder AddDriverConfiguration(IConfiguration configuration)
        {
            _appParametersBuilder.AddDriverConfiguration(configuration);
            return this;
        }

        /// <summary>
        /// Add the assembly needed for the given Type to the driver.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public JobRequestBuilder AddLocalAssemblyForType(Type type)
        {
            _appParametersBuilder.AddLocalAssemblyForType(type);
            return this;
        }

        /// <summary>
        /// Add the assembly needed for the given Type to all containers.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public JobRequestBuilder AddGlobalAssemblyForType(Type type)
        {
            _appParametersBuilder.AddGlobalAssemblyForType(type);
            return this;
        }

        /// <summary>
        /// Gives the job an identifier.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public JobRequestBuilder SetJobIdentifier(string id)
        {
            _jobParametersBuilder.SetJobIdentifier(id);
            return this;
        }

        /// <summary>
        /// Sets the amount of memory (in MB) to allocate for the Driver.
        /// </summary>
        /// <param name="driverMemoryInMb">The amount of memory (in MB) to allocate for the Driver.</param>
        /// <returns>this</returns>
        public JobRequestBuilder SetDriverMemory(int driverMemoryInMb)
        {
            _jobParametersBuilder.SetDriverMemory(driverMemoryInMb);
            return this;
        }

        /// <summary>
        /// Set a job submission environment variable.
        /// </summary>
        /// <param name="key">key of the environment variable.</param>
        /// <param name="value">Value of the environment variable.</param>
        /// <returns></returns>
        public JobRequestBuilder SetJobSubmissionEnvironmentVariable(string key, string value)
        {
            _jobParametersBuilder.SetJobSubmissionEnvironmentVariable(key, value);
            return this;
        }

        /// <summary>
        /// Sets the maximum amount of times a job can be submitted.
        /// </summary>
        public JobRequestBuilder SetMaxApplicationSubmissions(int maxAppSubmissions)
        {
            _jobParametersBuilder.SetMaxApplicationSubmissions(maxAppSubmissions);
            return this;
        }

        /// <summary>
        /// Sets the stdout file path for the driver.
        /// </summary>
        public JobRequestBuilder SetDriverStdoutFilePath(string driverStdoutFilePath)
        {
            _jobParametersBuilder.SetDriverStdoutFilePath(driverStdoutFilePath);
            return this;
        }

        /// <summary>
        /// Sets the stderr file path for the driver.
        /// </summary>
        public JobRequestBuilder SetDriverStderrFilePath(string driverStderrFilePath)
        {
            _jobParametersBuilder.SetDriverStderrFilePath(driverStderrFilePath);
            return this;
        }

        /// <summary>
        /// Driver config file contents (Org.Apache.REEF.Bridge.exe.config) contents
        /// Can be use to redirect assembly versions
        /// </summary>
        /// <param name="driverConfigurationFileContents">Driver configuration file contents.</param>
        /// <returns>this</returns>
        public JobRequestBuilder SetDriverConfigurationFileContents(string driverConfigurationFileContents)
        {
            _appParametersBuilder.SetDriverConfigurationFileContents(driverConfigurationFileContents);
            return this;
        }

        public JobRequestBuilder AddDriverConfigurationProviders(IEnumerable<IConfigurationProvider> configurationProviders)
        {
            _appParametersBuilder.AddDriverConfigurationProviders(configurationProviders);
            return this;
        }

        public JobRequestBuilder SetJavaLogLevel(JavaLoggingSetting javaLogLevel)
        {
            _jobParametersBuilder.SetJavaLogLevel(javaLogLevel);
            return this;
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

        private static void ThrowIfFileDoesnotExist(string path, string fileDescription)
        {
            if (!File.Exists(path))
            {
                throw new FileNotFoundException(string.Format("{0} not Found: {1}", fileDescription, path));
            }
        }
    }
}
