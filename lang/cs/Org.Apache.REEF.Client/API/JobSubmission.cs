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
    /// Captures a submission of a REEF Job to a cluster.
    /// </summary>
    internal sealed class JobSubmission : IJobSubmission
    {
        private readonly ISet<IConfiguration> _driverConfigurations;
        private readonly ISet<string> _globalAssemblies;
        private readonly ISet<string> _globalFiles;
        private readonly ISet<string> _localAssemblies;
        private readonly ISet<string> _localFiles;
        private readonly int _driverMemory;
        private readonly int _maxAppSubmissions;
        private readonly string _jobIdentifier;
        private readonly string _driverConfigurationFileContents;

        internal JobSubmission(
            ISet<IConfiguration> driverConfigurations,
            ISet<string> globalAssemblies,
            ISet<string> globalFiles,
            ISet<string> localAssemblies,
            ISet<string> localFiles,
            int driverMemory,
            string jobIdentifier,
            string driverConfigurationFileContents,
            int maxAppSubmissions)
        {
            _driverConfigurations = driverConfigurations;
            _globalAssemblies = globalAssemblies;
            _globalFiles = globalFiles;
            _localAssemblies = localAssemblies;
            _localFiles = localFiles;
            _driverMemory = driverMemory;
            _jobIdentifier = jobIdentifier;
            _driverConfigurationFileContents = driverConfigurationFileContents;
            _maxAppSubmissions = maxAppSubmissions;
        }

        /// <summary>
        /// The assemblies to be made available to all containers.
        /// </summary>
        ISet<string> IJobSubmission.GlobalAssemblies
        {
            get { return _globalAssemblies; }
        }

        /// <summary>
        /// The driver configurations
        /// </summary>
        ISet<IConfiguration> IJobSubmission.DriverConfigurations
        {
            get { return _driverConfigurations; }
        }

        ISet<string> IJobSubmission.GlobalFiles
        {
            get { return _globalFiles; }
        }

        ISet<string> IJobSubmission.LocalAssemblies
        {
            get { return _localAssemblies; }
        }

        ISet<string> IJobSubmission.LocalFiles
        {
            get { return _localFiles; }
        }

        int IJobSubmission.DriverMemory
        {
            get { return _driverMemory; }
        }

        int IJobSubmission.MaxApplicationSubmissions
        {
            get { return _maxAppSubmissions; }
        }

        /// <summary>
        /// The Job's identifier
        /// </summary>
        string IJobSubmission.JobIdentifier 
        {
            get { return _jobIdentifier; }
        }

        /// <summary>
        /// Driver config file contents (Org.Apache.REEF.Bridge.exe.config)
        /// Can be use to redirect assembly versions
        /// </summary>
        string IJobSubmission.DriverConfigurationFileContents
        {
            get { return _driverConfigurationFileContents; }
        }
    }
}