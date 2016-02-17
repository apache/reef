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
            return new JobParameters(_jobIdentifier, _maxApplicationSubmissions, _driverMemory);
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
    }
}