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
using System.Threading.Tasks;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Client.YARN
{
    /// <summary>
    /// Interface that defines Yarn client API
    /// </summary>
    public interface IYarnREEFClient : IREEFClient
    {
        /// <summary>
        /// Returns all the application reports running in the cluster
        /// </summary>
        /// <returns></returns>
        [Unstable("0.17", "Working in progress for rest API id returned")]
        Task<IReadOnlyDictionary<string, IApplicationReport>> GetApplicationReports();

        /// <summary>
        /// Kills the job application and return Job status
        /// </summary>
        /// <param name="appId"></param>
        /// <returns></returns>
        [Unstable("0.17", "Working in progress for rest API status returned")]
        Task<FinalState> KillJobApplication(string appId);
    }
}
