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
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Client.YARN.RestClient;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.AsyncUtils;
using RestSharp;

namespace Org.Apache.REEF.Client.Yarn.RestClient
{
    /// <summary>
    /// Implements YARN client APIs analogous to the REST APIs
    /// provided by YARN.
    /// The non-cancellation token versions are implemented in
    /// YarnClientNoCancellationToken.
    /// </summary>
    internal sealed partial class YarnClient : IYarnRMClient
    {
        private readonly string _baseResourceString;
        private readonly IUrlProvider _yarnRmUrlProviderUri;
        private readonly IRestRequestExecutor _restRequestExecutor;

        [Inject]
        internal YarnClient(IUrlProvider yarnRmUrlProviderUri, IRestRequestExecutor restRequestExecutor)
        {
            _yarnRmUrlProviderUri = yarnRmUrlProviderUri;
            _baseResourceString = @"ws/v1/";
            _restRequestExecutor = restRequestExecutor;
        }

        public async Task<ClusterInfo> GetClusterInfoAsync(CancellationToken cancellationToken)
        {
            await new RemoveSynchronizationContextAwaiter();

            IRestRequest request = new RestRequest
            {
                Resource = _baseResourceString + ClusterInfo.Resource,
                RootElement = ClusterInfo.RootElement
            };

            return
                await GenerateUrlAndExecuteRequestAsync<ClusterInfo>(request, cancellationToken);
        }

        public async Task<ClusterMetrics> GetClusterMetricsAsync(CancellationToken cancellationToken)
        {
            await new RemoveSynchronizationContextAwaiter();

            var request = new RestRequest
            {
                Resource = _baseResourceString + ClusterMetrics.Resource,
                RootElement = ClusterMetrics.RootElement
            };

            return
                await
                    GenerateUrlAndExecuteRequestAsync<ClusterMetrics>(request, cancellationToken);
        }

        public async Task<Application> GetApplicationAsync(string appId, CancellationToken cancellationToken)
        {
            await new RemoveSynchronizationContextAwaiter();

            var request = new RestRequest
            {
                Resource = _baseResourceString + Application.Resource + appId,
                RootElement = Application.RootElement
            };

            return
                await GenerateUrlAndExecuteRequestAsync<Application>(request, cancellationToken);
        }

        public async Task<NewApplication> CreateNewApplicationAsync(CancellationToken cancellationToken)
        {
            await new RemoveSynchronizationContextAwaiter();

            var request = new RestRequest
            {
                Resource = _baseResourceString + NewApplication.Resource,
                Method = Method.POST
            };

            return
                await
                    GenerateUrlAndExecuteRequestAsync<NewApplication>(request, cancellationToken);
        }

        public async Task<Application> SubmitApplicationAsync(
            SubmitApplication submitApplication,
            CancellationToken cancellationToken)
        {
            await new RemoveSynchronizationContextAwaiter();

            var request = new RestRequest
            {
                Resource = _baseResourceString + SubmitApplication.Resource,
                Method = Method.POST,
                RequestFormat = DataFormat.Json,
                JsonSerializer = new RestJsonSerializer()
            };

            request.AddBody(submitApplication);
            var submitResponse = await GenerateUrlAndExecuteRequestAsync(request, cancellationToken);

            if (submitResponse.StatusCode != HttpStatusCode.Accepted)
            {
                throw new YarnRestAPIException(string.Format("Application submission failed with HTTP STATUS {0}",
                    submitResponse.StatusCode));
            }

            return await GetApplicationAsync(submitApplication.ApplicationId, cancellationToken);
        }


        private async Task<T> GenerateUrlAndExecuteRequestAsync<T>(IRestRequest request,
            CancellationToken cancellationToken)
            where T : new()
        {
            Uri yarnRmUri = await _yarnRmUrlProviderUri.GetUrlAsync();
            return
                await
                    _restRequestExecutor.ExecuteAsync<T>(request, yarnRmUri, cancellationToken);
        }

        private async Task<IRestResponse> GenerateUrlAndExecuteRequestAsync(IRestRequest request,
            CancellationToken cancellationToken)
        {
            Uri yarnRmUri = await _yarnRmUrlProviderUri.GetUrlAsync();
            return
                await
                    _restRequestExecutor.ExecuteAsync(request, yarnRmUri, cancellationToken);
        }
    }
}