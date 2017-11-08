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

using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Client.Common
{
    [Unstable("0.13", "Working in progress. For local runtime, some of the property, such as FinalState and AppId are not implemented yet.")]
    public interface IJobSubmissionResult
    {
        /// <summary>
        /// Get http response for the given url
        /// </summary>
        /// <param name="url"></param>
        /// <returns></returns>
        string GetUrlResult(string url);

        /// <summary>
        /// This method returns the url of http server running inside the driver.
        /// e.g. http://hostname:port/
        /// </summary>
        string DriverUrl { get; }

        /// <summary>
        /// Get Application final state
        /// </summary>
        FinalState FinalState { get; }

        /// <summary>
        /// Get Yarn application id after Job is submited
        /// </summary>
        string AppId { get; }

        /// <summary>
        /// Waits for the Driver to complete.
        /// </summary>
        /// <exception cref="System.Net.WebException">If the Driver could be reached at least once.</exception>
        [Unstable("0.17", "Uses the HTTP server in the Java Driver. Might not work if that cannot be reached.")]
        void WaitForDriverToFinish();
    }
}
