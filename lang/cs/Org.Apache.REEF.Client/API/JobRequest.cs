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
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Client.API
{
    /// <summary>
    /// Describes parameters for a single job submission.
    /// </summary>
    public sealed class JobRequest
    {
        private readonly JobParameters _jobParameters;
        private readonly AppParameters _appParameters;

        internal JobRequest(JobParameters jobParameters, AppParameters appParameters)
        {
            _jobParameters = jobParameters;
            _appParameters = appParameters;
        }

        [Obsolete("Introduced to bridge deprecation of IJobSubmission.")]
        internal static JobRequest FromJobSubmission(IJobSubmission jobSubmission)
        {
            return new JobRequest(
                JobParameters.FromJobSubmission(jobSubmission), AppParameters.FromJobSubmission(jobSubmission));
        }

        /// <summary>
        /// The assemblies to be made available to all containers.
        /// </summary>
        public ISet<string> GlobalAssemblies
        {
            get { return _appParameters.GlobalAssemblies; }
        }

        /// <summary>
        /// The driver configurations
        /// </summary>
        public ISet<IConfiguration> DriverConfigurations
        {
            get { return _appParameters.DriverConfigurations; }
        }

        /// <summary>
        /// The global files to be made available to all containers.
        /// </summary>
        public ISet<string> GlobalFiles
        {
            get { return _appParameters.GlobalFiles; }
        }

        /// <summary>
        /// The assemblies to be made available only to the local container.
        /// </summary>
        public ISet<string> LocalAssemblies
        {
            get { return _appParameters.LocalAssemblies; }
        }

        /// <summary>
        /// The files to be made available only to the local container.
        /// </summary>
        public ISet<string> LocalFiles
        {
            get { return _appParameters.LocalFiles; }
        }

        /// <summary>
        /// The size of the driver memory, in MB.
        /// </summary>
        public int DriverMemory
        {
            get { return _jobParameters.DriverMemoryInMB; }
        }

        /// <summary>
        /// The maximum amount of times the job can be submitted. Used primarily in the 
        /// driver restart scenario.
        /// </summary>
        public int MaxApplicationSubmissions
        {
            get { return _jobParameters.MaxApplicationSubmissions; }
        }

        /// <summary>
        /// The Job's identifier
        /// </summary>
        public string JobIdentifier
        {
            get { return _jobParameters.JobIdentifier; }
        }

        /// <summary>
        /// Driver config file contents (Org.Apache.REEF.Bridge.exe.config)
        /// Can be use to redirect assembly versions
        /// </summary>
        public string DriverConfigurationFileContents
        {
            get { return _appParameters.DriverConfigurationFileContents; }
        }

        /// <summary>
        /// Gets the <see cref="JobParameters"/> for this particular job submission.
        /// </summary>
        public JobParameters JobParameters
        {
            get { return _jobParameters; }
        }

        /// <summary>
        /// Gets the <see cref="AppParameters"/> for this particular job submission.
        /// </summary>
        public AppParameters AppParameters
        {
            get { return _appParameters; }
        }
    }
}