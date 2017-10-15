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
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.API
{
    /// <summary>
    /// The parameters for a REEF job, used to specify job parameters on each REEF submission.
    /// For application parameters which is specified only once for all job submissions of the same
    /// REEF application, see <see cref="AppParameters"/>.
    /// </summary>
    public sealed class JobParameters
    {
        private readonly string _jobIdentifier;
        private readonly int _maxApplicationSubmissions;
        private readonly int _driverMemory;
        private IDictionary<string, string> _jobSubmissionEnvMap;
        private readonly Optional<string> _stdoutFilePath;
        private readonly Optional<string> _stderrFilePath;
        private readonly JavaLoggingSetting _logSetting;

        internal JobParameters(
            string jobIdentifier, 
            int maxApplicationSubmissions, 
            int driverMemory,
            IDictionary<string, string> jobSubmissionEnvMap, 
            string stdoutFilePath,
            string stderrFilePath,
            JavaLoggingSetting logSetting)
        {
            _jobIdentifier = jobIdentifier;
            _maxApplicationSubmissions = maxApplicationSubmissions;
            _driverMemory = driverMemory;
            _jobSubmissionEnvMap = jobSubmissionEnvMap;

            _stdoutFilePath = string.IsNullOrWhiteSpace(stdoutFilePath) ? 
                Optional<string>.Empty() : Optional<string>.Of(stdoutFilePath);

            _stderrFilePath = string.IsNullOrWhiteSpace(stderrFilePath) ?
                Optional<string>.Empty() : Optional<string>.Of(stderrFilePath);

            _logSetting = logSetting;
        }

        /// <summary>
        /// The identifier of the job.
        /// </summary>
        public string JobIdentifier
        {
            get { return _jobIdentifier; }
        }

        /// <summary>
        /// The maximum amount of times the job can be submitted. Used primarily in the 
        /// driver restart scenario.
        /// </summary>
        public int MaxApplicationSubmissions
        {
            get { return _maxApplicationSubmissions;  }
        }

        /// <summary>
        /// The size of the driver memory, in MB.
        /// </summary>
        public int DriverMemoryInMB
        {
            get { return _driverMemory; }
        }

        /// <summary>
        /// The job submission environment variable map.
        /// </summary>
        public IDictionary<string, string> JobSubmissionEnvMap
        {
            get { return new Dictionary<string, string>(_jobSubmissionEnvMap); }
        }

        /// <summary>
        /// Gets the file path for stdout for the driver.
        /// </summary>
        public Optional<string> StdoutFilePath
        {
            get { return _stdoutFilePath; }
        } 

        /// <summary>
        /// Gets the file path for stderr for the driver.
        /// </summary>
        public Optional<string> StderrFilePath
        {
            get { return _stderrFilePath; }
        }

        /// <summary>
        /// Gets the Java log level.
        /// </summary>
        public JavaLoggingSetting JavaLogLevel
        {
            get { return _logSetting; }
        }
    }
}