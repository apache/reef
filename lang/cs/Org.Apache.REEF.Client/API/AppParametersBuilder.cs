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
    /// <summary>
    /// A builder for <see cref="AppParameters"/>.
    /// </summary>
    public sealed class AppParametersBuilder
    {
        private readonly ISet<IConfiguration> _driverConfigurations = new HashSet<IConfiguration>();
        private readonly ISet<string> _globalAssemblies = new HashSet<string>();
        private readonly ISet<string> _globalFiles = new HashSet<string>();
        private readonly ISet<string> _localAssemblies = new HashSet<string>();
        private readonly ISet<string> _localFiles = new HashSet<string>();
        private readonly ISet<IConfigurationProvider> _configurationProviders = new HashSet<IConfigurationProvider>();

        private string _driverConfigurationFileContents;

        private AppParametersBuilder()
        {
        }

        /// <summary>
        /// Creates a new <see cref="AppParametersBuilder"/>.
        /// </summary>
        public static AppParametersBuilder NewBuilder()
        {
            return new AppParametersBuilder();
        }

        /// <summary>
        /// Add a file to be made available in all containers.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public AppParametersBuilder AddGlobalFile(string fileName)
        {
            _globalFiles.Add(fileName);
            return this;
        }

        /// <summary>
        /// Add a file to be made available only on the driver.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public AppParametersBuilder AddLocalFile(string fileName)
        {
            _localFiles.Add(fileName);
            return this;
        }

        /// <summary>
        /// Add an assembly to be made available on all containers.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public AppParametersBuilder AddGlobalAssembly(string fileName)
        {
            _globalAssemblies.Add(fileName);
            return this;
        }

        /// <summary>
        /// Add an assembly to the driver only.
        /// </summary>
        /// <param name="fileName"></param>
        /// <returns></returns>
        public AppParametersBuilder AddLocalAssembly(string fileName)
        {
            _localAssemblies.Add(fileName);
            return this;
        }

        /// <summary>
        /// Add a Configuration to the Driver.
        /// </summary>
        public AppParametersBuilder AddDriverConfiguration(IConfiguration configuration)
        {
            _driverConfigurations.Add(configuration);
            return this;
        }

        /// <summary>
        /// Adds a driver configuration provider for configurations of the Driver.
        /// </summary>
        public AppParametersBuilder AddDriverConfigurationProviders(
            IEnumerable<IConfigurationProvider> configurationProviders)
        {
            _configurationProviders.UnionWith(configurationProviders);
            return this;
        }

        /// <summary>
        /// Add the assembly needed for the given Type to the driver.
        /// </summary>
        public AppParametersBuilder AddLocalAssemblyForType(Type type)
        {
            AddLocalAssembly(GetAssemblyPathForType(type));
            return this;
        }

        /// <summary>
        /// Add the assembly needed for the given Type to all containers.
        /// </summary>
        public AppParametersBuilder AddGlobalAssemblyForType(Type type)
        {
            AddGlobalAssembly(GetAssemblyPathForType(type));
            return this;
        }

        /// <summary>
        /// Driver config file contents (Org.Apache.REEF.Bridge.exe.config) contents
        /// Can be use to redirect assembly versions
        /// </summary>
        /// <param name="driverConfigurationFileContents">Driver configuration file contents.</param>
        public AppParametersBuilder SetDriverConfigurationFileContents(string driverConfigurationFileContents)
        {
            _driverConfigurationFileContents = driverConfigurationFileContents;
            return this;
        }

        /// <summary>
        /// Finds the path to the assembly the given Type was loaded from.
        /// </summary>
        private static string GetAssemblyPathForType(Type type)
        {
            var path = Uri.UnescapeDataString(new UriBuilder(type.Assembly.CodeBase).Path);
            return Path.GetFullPath(path);
        }

        /// <summary>
        /// Builds the application parameters.
        /// </summary>
        public AppParameters Build()
        {
            foreach (var conf in _configurationProviders)
            {
                _driverConfigurations.Add(conf.GetConfiguration());
            }
            return new AppParameters(_driverConfigurations, _globalAssemblies, _globalFiles, _localAssemblies,
                _localFiles, _driverConfigurationFileContents);
        }
    }
}