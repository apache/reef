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
using System.IO;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Client.API
{
    internal class JobSubmissionBuilder : IJobSubmissionBuilder
    {
        private readonly ISet<IConfiguration> _driverConfigurations = new HashSet<IConfiguration>();
        private readonly ISet<string> _globalAssemblies = new HashSet<string>();
        private readonly ISet<string> _globalFiles = new HashSet<string>();
        private readonly ISet<string> _localAssemblies = new HashSet<string>();
        private readonly ISet<string> _localFiles = new HashSet<string>();
        private int _driverMemory = 512;
        private string _jobIdentifier;
        private readonly ISet<IConfigurationProvider> _configurationProviders;
        private string _driverConfigurationFileContents;

        internal JobSubmissionBuilder(ISet<IConfigurationProvider> configurationProviders)
        {
            _configurationProviders = configurationProviders;
        }

        /// <summary>
        /// Add a file to be made available in all containers.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public IJobSubmissionBuilder AddGlobalFile(string fileName)
        {
            _globalFiles.Add(fileName);
            return this;
        }

        /// <summary>
        /// Add a file to be made available only on the driver.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public IJobSubmissionBuilder AddLocalFile(string fileName)
        {
            _localFiles.Add(fileName);
            return this;
        }

        /// <summary>
        /// Add an assembly to be made available on all containers.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public IJobSubmissionBuilder AddGlobalAssembly(string fileName)
        {
            _globalAssemblies.Add(fileName);
            return this;
        }

        /// <summary>
        /// Add an assembly to the driver only.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public IJobSubmissionBuilder AddLocalAssembly(string fileName)
        {
            _localAssemblies.Add(fileName);
            return this;
        }

        /// <summary>
        /// Add a Configuration to the Driver.
        /// </summary>
        /// <param name="configuration"></param>
        /// <returns></returns>
        public IJobSubmissionBuilder AddDriverConfiguration(IConfiguration configuration)
        {
            _driverConfigurations.Add(configuration);
            return this;
        }

        /// <summary>
        /// Add the assembly needed for the given Type to the driver.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public IJobSubmissionBuilder AddLocalAssemblyForType(Type type)
        {
            AddLocalAssembly(GetAssemblyPathForType(type));
            return this;
        }

        /// <summary>
        /// Add the assembly needed for the given Type to all containers.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public IJobSubmissionBuilder AddGlobalAssemblyForType(Type type)
        {
            AddGlobalAssembly(GetAssemblyPathForType(type));
            return this;
        }

        /// <summary>
        /// Gives the job an identifier.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public IJobSubmissionBuilder SetJobIdentifier(string id)
        {
            _jobIdentifier = id;
            return this;
        }

        /// <summary>
        /// Sets the amount of memory (in MB) to allocate for the Driver.
        /// </summary>
        /// <param name="driverMemoryInMb">The amount of memory (in MB) to allocate for the Driver.</param>
        /// <returns>this</returns>
        public IJobSubmissionBuilder SetDriverMemory(int driverMemoryInMb)
        {
            _driverMemory = driverMemoryInMb;
            return this;
        }

        /// <summary>
        /// Driver config file contents (Org.Apache.REEF.Bridge.exe.config) contents
        /// Can be use to redirect assembly versions
        /// </summary>
        /// <param name="driverConfigurationFileContents">Driver configuration file contents.</param>
        /// <returns>this</returns>
        public IJobSubmissionBuilder SetDriverConfigurationFileContents(string driverConfigurationFileContents)
        {
            _driverConfigurationFileContents = driverConfigurationFileContents;
            return this;
        }

        /// <summary>
        /// Finds the path to the assembly the given Type was loaded from.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private static string GetAssemblyPathForType(Type type)
        {
            var path = Uri.UnescapeDataString(new UriBuilder(type.Assembly.CodeBase).Path);
            return Path.GetFullPath(path);
        }

        /// <summary>
        /// Builds the submission
        /// </summary>
        /// <returns>IJobSubmission</returns>
        public IJobSubmission Build()
        {
            foreach (var cfg in _configurationProviders)
            {
                _driverConfigurations.Add(cfg.GetConfiguration());
            }

            return new JobSubmission(_driverConfigurations, _globalAssemblies, _globalFiles, _localAssemblies,
                _localFiles, _driverMemory, _jobIdentifier, _driverConfigurationFileContents);
        }
    }
}
