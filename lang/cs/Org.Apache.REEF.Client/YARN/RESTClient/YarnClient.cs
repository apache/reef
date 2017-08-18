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
using System.Globalization;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Client.YARN.RestClient;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.AsyncUtils;
using Org.Apache.REEF.Utilities.Logging;

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
        private static readonly Logger Logger = Logger.GetLogger(typeof(YarnClient));
        private readonly IUrlProvider _yarnRmUrlProviderUri;
        private readonly IRestRequestExecutor _restRequestExecutor;
        private readonly IRequestFactory _requestFactory;

        [Inject]
        private YarnClient(
            IUrlProvider yarnRmUrlProviderUri,
            IRestRequestExecutor restRequestExecutor,
            IRequestFactory requestFactory)
        {
            _requestFactory = requestFactory;
            _yarnRmUrlProviderUri = yarnRmUrlProviderUri;
            _restRequestExecutor = restRequestExecutor;
        }

        public async Task<ClusterInfo> GetClusterInfoAsync(CancellationToken cancellationToken)
        {
            await new RemoveSynchronizationContextAwaiter();

            var request = _requestFactory.CreateRestRequest(
                ClusterInfo.Resource,
                Method.GET,
                ClusterInfo.RootElement);

            return
                await GenerateUrlAndExecuteRequestAsync<ClusterInfo>(request, cancellationToken);
        }

        public async Task<ClusterMetrics> GetClusterMetricsAsync(CancellationToken cancellationToken)
        {
            await new RemoveSynchronizationContextAwaiter();

            var request = _requestFactory.CreateRestRequest(ClusterMetrics.Resource,
                Method.GET,
                ClusterMetrics.RootElement);

            return
                await
                    GenerateUrlAndExecuteRequestAsync<ClusterMetrics>(request, cancellationToken);
        }

        public async Task<Application> GetApplicationAsync(string appId, CancellationToken cancellationToken)
        {
            await new RemoveSynchronizationContextAwaiter();

            var request = _requestFactory.CreateRestRequest(Application.Resource + appId,
                Method.GET,
                Application.RootElement);

            return
                await GenerateUrlAndExecuteRequestAsync<Application>(request, cancellationToken);
        }

        /// <summary>
        /// This API returns information about all the applications maintained
        /// by YARN RM in the cluster by invoking REST API. This API also allow cooperative
        /// cancellation in multi-threading scenarios.
        /// </summary>
        /// <param name="cancellationToken">cancellation token</param>
        /// <returns>list of applications</returns>
        public async Task<Applications> GetApplicationsAsync(CancellationToken cancellationToken)
        {
            await new RemoveSynchronizationContextAwaiter();

            var request = _requestFactory.CreateRestRequest(Applications.Resource,
                Method.GET,
                Applications.RootElement);

            return
                await GenerateUrlAndExecuteRequestAsync<Applications>(request, cancellationToken);
        }

        public async Task<NewApplication> CreateNewApplicationAsync(CancellationToken cancellationToken)
        {
            await new RemoveSynchronizationContextAwaiter();

            var request = _requestFactory.CreateRestRequest(NewApplication.Resource, Method.POST);

            return
                await
                    GenerateUrlAndExecuteRequestAsync<NewApplication>(request, cancellationToken);
        }

        public async Task<Application> SubmitApplicationAsync(
            SubmitApplication submitApplication,
            CancellationToken cancellationToken)
        {
            await new RemoveSynchronizationContextAwaiter();

            var request = _requestFactory.CreateRestRequest(
                SubmitApplication.Resource,
                Method.POST,
                rootElement: null,
                body: submitApplication);

            var submitResponse = await GenerateUrlAndExecuteRequestAsync(request, cancellationToken);

            if (submitResponse.StatusCode != HttpStatusCode.Accepted)
            {
                throw new YarnRestAPIException(
                    string.Format("Application submission failed with HTTP STATUS {0}",
                        submitResponse.StatusCode));
            }

            return await GetApplicationAsync(submitApplication.ApplicationId, cancellationToken);
        }

        /// <summary>
        /// Kills the application asynchronous.
        /// </summary>
        /// <param name="appId">The application identifier.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <exception cref="System.NotImplementedException"></exception>
        public async void KillApplicationAsync(string appId, CancellationToken cancellationToken)
        {
            try
            {
                var killApplication = new KillApplication();
                killApplication.State = State.KILLED;

                var restParm = KillApplication.Resource + appId + KillApplication.StateTag;
                await new RemoveSynchronizationContextAwaiter();
                var request = _requestFactory.CreateRestRequest(
                    restParm,
                    Method.PUT,
                    rootElement: null,
                    body: killApplication);

                var response = await GenerateUrlAndExecuteRequestAsync(request, cancellationToken);
                
                Logger.Log(Level.Info, "StatueCode from response {0}", response.StatusCode);

                if (response.StatusCode != HttpStatusCode.Accepted)
                {
                    throw new YarnRestAPIException(
                        string.Format("Kill Application failed with HTTP STATUS {0}",
                            response.StatusCode));
                }
            }
            catch (Exception e)
            {
                Logger.Log(Level.Error, "YarnClient:KillApplicationAsync got exception", e);
                throw e;
            }
        }

        private async Task<T> GenerateUrlAndExecuteRequestAsync<T>(RestRequest request,
            CancellationToken cancellationToken)
        {
            IEnumerable<Uri> yarnRmUris = await _yarnRmUrlProviderUri.GetUrlAsync();
            var exceptions = new List<Exception>();
            foreach (var yarnRmUri in yarnRmUris)
            {
                try
                {
                    return
                        await
                            _restRequestExecutor.ExecuteAsync<T>(request, yarnRmUri, cancellationToken);
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                    Logger.Log(Level.Verbose,
                        string.Format(CultureInfo.CurrentCulture, "Possibly transient error in rest call {0}", e.Message));
                }
            }

            throw new AggregateException("Failed Rest Request", exceptions);
        }

        private async Task<RestResponse<VoidResult>> GenerateUrlAndExecuteRequestAsync(RestRequest request,
            CancellationToken cancellationToken)
        {
            IEnumerable<Uri> yarnRmUris = await _yarnRmUrlProviderUri.GetUrlAsync();
            var exceptions = new List<Exception>();
            foreach (var yarnRmUri in yarnRmUris)
            {
                try
                {
                    return
                        await
                            _restRequestExecutor.ExecuteAsync(request, yarnRmUri, cancellationToken);
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                    Logger.Log(Level.Verbose,
                        string.Format(CultureInfo.CurrentCulture, "Possibly transient error in rest call {0}", e.Message));
                }
            }

            throw new AggregateException("Failed Rest Request", exceptions);
        }
    }
}