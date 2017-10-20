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
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.API
{
    /// <summary>
    /// A builder for <see cref="JobParameters"/>.
    /// </summary>
    public sealed class JobParametersBuilder
    {
        private string _jobIdentifier;
        private int _maxApplicationSubmissions = 1;
        private int _driverMemory = 512;
        private string _stdoutFilePath = null;
        private string _stderrFilePath = null;
        private readonly IDictionary<string, string> _jobSubmissionMap = new Dictionary<string, string>();
        private JavaLoggingSetting _javaLogLevel = JavaLoggingSetting.Info;

        private JobParametersBuilder()
        {
        }

        /// <summary>
        /// Creates a new <see cref="JobParametersBuilder"/>.
        /// </summary>
        public static JobParametersBuilder NewBuilder()
        {
            return new JobParametersBuilder();
        }

        /// <summary>
        /// Builds a <see cref="JobParameters"/> object based on parameters passed to the builder.
        /// </summary>
        /// <returns></returns>
        public JobParameters Build()
        {
            return new JobParameters(
                _jobIdentifier, 
                _maxApplicationSubmissions, 
                _driverMemory,
                _jobSubmissionMap,
                _stdoutFilePath,
                _stderrFilePath,
                _javaLogLevel);
        }

        /// <summary>
        /// Sets the identifier of the job.
        /// </summary>
        public JobParametersBuilder SetJobIdentifier(string id)
        {
            _jobIdentifier = id;
            return this;
        }

        /// <summary>
        /// Sets the maximum amount of times the job can be submitted. Used primarily in the 
        /// driver restart scenario.
        /// </summary>
        public JobParametersBuilder SetMaxApplicationSubmissions(int maxApplicationSubmissions)
        {
            _maxApplicationSubmissions = maxApplicationSubmissions;
            return this;
        }

        /// <summary>
        /// Sets the amount of memory (in MB) to allocate for the Driver.
        /// </summary>
        /// <param name="driverMemoryInMb">The amount of memory (in MB) to allocate for the Driver.</param>
        /// <returns>this</returns>
        public JobParametersBuilder SetDriverMemory(int driverMemoryInMb)
        {
            _driverMemory = driverMemoryInMb;
            return this;
        }

        /// <summary>
        /// Set job submission environment variable.
        /// If the variable is already in the map, override it. 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public JobParametersBuilder SetJobSubmissionEnvironmentVariable(string key, string value)
        {
            _jobSubmissionMap[key] = value;
            return this;
        }        

        /// <summary>
        /// Sets the file path to the stdout file for the driver.
        /// </summary>
        public JobParametersBuilder SetDriverStdoutFilePath(string stdoutFilePath)
        {
            _stdoutFilePath = stdoutFilePath;
            return this;
        }

        /// <summary>
        /// Sets the file path to the stderr file for the driver.
        /// </summary>
        public JobParametersBuilder SetDriverStderrFilePath(string stderrFilePath)
        {
            _stderrFilePath = stderrFilePath;
            return this;
        }

        /// <summary>
        /// Sets the Java Log Level.
        /// </summary>
        public JobParametersBuilder SetJavaLogLevel(JavaLoggingSetting javaLogLevel)
        {
            _javaLogLevel = javaLogLevel;
            return this;
        }
    }
}