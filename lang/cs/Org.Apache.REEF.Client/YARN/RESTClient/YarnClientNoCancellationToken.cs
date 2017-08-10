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

using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Utilities.AsyncUtils;

namespace Org.Apache.REEF.Client.Yarn.RestClient
{
    internal sealed partial class YarnClient
    {
        /// <summary>
        /// Get Cluster Info without cancellation token.
        /// </summary>
        public async Task<ClusterInfo> GetClusterInfoAsync()
        {
            await new RemoveSynchronizationContextAwaiter();
            return await GetClusterInfoAsync(CancellationToken.None);
        }

        /// <summary>
        /// Get Cluster Metrics without cancellation token.
        /// </summary>
        public async Task<ClusterMetrics> GetClusterMetricsAsync()
        {
            await new RemoveSynchronizationContextAwaiter();
            return await GetClusterMetricsAsync(CancellationToken.None);
        }

        /// <summary>
        /// Get Application without cancellation token.
        /// </summary>
        public async Task<Application> GetApplicationAsync(string appId)
        {
            await new RemoveSynchronizationContextAwaiter();
            return await GetApplicationAsync(appId, CancellationToken.None);
        }

        /// <summary>
        /// Get Applications without cancellation token.
        /// </summary>
        public async Task<Applications> GetApplicationsAsync()
        {
            await new RemoveSynchronizationContextAwaiter();
            return await GetApplicationsAsync(CancellationToken.None);
        }

        /// <summary>
        /// Create a new Application without cancellation token.
        /// </summary>
        public async Task<NewApplication> CreateNewApplicationAsync()
        {
            await new RemoveSynchronizationContextAwaiter();
            return await CreateNewApplicationAsync(CancellationToken.None);
        }

        /// <summary>
        /// Submit Application without cancellation token.
        /// </summary>
        /// <param name="submitApplicationRequest"></param>
        public async Task<Application> SubmitApplicationAsync(SubmitApplication submitApplicationRequest)
        {
            await new RemoveSynchronizationContextAwaiter();
            return await SubmitApplicationAsync(submitApplicationRequest, CancellationToken.None);
        }
    }
}