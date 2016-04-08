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
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Client.API
{
    /// <summary>
    /// The parameters for a REEF application, used to specify application parameters for each REEF application.
    /// For job parameters which is specified each time for job submissions of the same
    /// REEF application, see <see cref="JobParameters"/>.
    /// </summary>
    public sealed class AppParameters
    {
        private readonly ISet<IConfiguration> _driverConfigurations;
        private readonly ISet<string> _globalAssemblies;
        private readonly ISet<string> _globalFiles;
        private readonly ISet<string> _localAssemblies;
        private readonly ISet<string> _localFiles;
        private readonly string _driverConfigurationFileContents;

        internal AppParameters(
            ISet<IConfiguration> driverConfigurations,
            ISet<string> globalAssemblies,
            ISet<string> globalFiles,
            ISet<string> localAssemblies,
            ISet<string> localFiles,
            string driverConfigurationFileContents)
        {
            _driverConfigurations = driverConfigurations;
            _globalAssemblies = globalAssemblies;
            _globalFiles = globalFiles;
            _localAssemblies = localAssemblies;
            _localFiles = localFiles;
            _driverConfigurationFileContents = driverConfigurationFileContents;
        }

        /// <summary>
        /// The assemblies to be made available to all containers.
        /// </summary>
        public ISet<string> GlobalAssemblies
        {
            get { return _globalAssemblies; }
        }

        /// <summary>
        /// The driver configurations.
        /// </summary>
        public ISet<IConfiguration> DriverConfigurations
        {
            get { return _driverConfigurations; }
        }

        /// <summary>
        /// The global files to be made available to all containers.
        /// </summary>
        public ISet<string> GlobalFiles
        {
            get { return _globalFiles; }
        }

        /// <summary>
        /// The assemblies to be made available only to the local container.
        /// </summary>
        public ISet<string> LocalAssemblies
        {
            get { return _localAssemblies; }
        }

        /// <summary>
        /// The files to be made available only to the local container.
        /// </summary>
        public ISet<string> LocalFiles
        {
            get { return _localFiles; }
        }

        /// <summary>
        /// Driver config file contents (Org.Apache.REEF.Bridge.exe.config)
        /// Can be use to redirect assembly versions
        /// </summary>
        public string DriverConfigurationFileContents
        {
            get { return _driverConfigurationFileContents; }
        }
    }
}