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

using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Client.API.Testing
{
    /// <summary>
    /// Runs REEF integration tests and reports their results.
    /// </summary>
    [Unstable("0.17", "Work in progress towards a new test infrastructure. See REEF-1271.")]
    public interface ITestRunner
    {
        /// <summary>
        /// Create a new JobRequestBuilder.
        /// </summary>
        /// <remarks>This may pre-configure the job request for this test runner.</remarks>
        /// <returns>A new JobRequestBuilder.</returns>
        JobRequestBuilder NewJobRequestBuilder();

        /// <summary>
        /// Runs the given Job as a test.
        /// </summary>
        /// <param name="jobRequestBuilder">The job to run.</param>
        /// <returns>The test results obtained.</returns>
        ITestResult RunTest(JobRequestBuilder jobRequestBuilder);
    }
}
