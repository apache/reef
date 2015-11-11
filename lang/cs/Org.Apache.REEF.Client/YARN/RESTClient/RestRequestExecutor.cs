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
using Newtonsoft.Json;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
using RestSharp;

namespace Org.Apache.REEF.Client.Yarn.RestClient
{
    internal class RestRequestExecutor : IRestRequestExecutor
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(RestRequestExecutor));
        private readonly IRestClientFactory _clientFactory;

        [Inject]
        private RestRequestExecutor(
            IRestClientFactory clientFactory)
        {
            _clientFactory = clientFactory;
        }

        public async Task<T> ExecuteAsync<T>(
            IRestRequest request,
            Uri requestUri,
            CancellationToken cancellationToken) where T : new()
        {
            var client = _clientFactory.CreateRestClient(requestUri);

            var response =
                await
                    client.ExecuteTaskAsync<T>(request, cancellationToken);

            if (response.ErrorException != null)
            {
                throw new YarnRestAPIException("Executing REST API failed", response.ErrorException);
            }

            try
            {
                // HTTP status code greater than 300 is unexpected here.
                // See if the server sent a error response and throw suitable
                // exception to user.
                if (response.StatusCode >= HttpStatusCode.Ambiguous)
                {
                    Log.Log(Level.Error, "RESTRequest failed. StatusCode: {0}; Response: {1}", response.StatusCode, response.Content);
                    var errorResponse = JsonConvert.DeserializeObject<Error>(response.Content);
                    throw new YarnRestAPIException { Error = errorResponse };
                }
            }
            catch (Exception exception)
            {
                throw new YarnRestAPIException("Unhandled exception in deserializing error response.", exception);
            }

            return response.Data;
        }

        public async Task<IRestResponse> ExecuteAsync(IRestRequest request, Uri uri, CancellationToken cancellationToken)
        {
            var client = _clientFactory.CreateRestClient(uri);

            try
            {
                return await client.ExecuteTaskAsync(request, cancellationToken);
            }
            catch (Exception exception)
            {
                throw new YarnRestAPIException("Unhandled exception in executing REST request.", exception);
            }
        }
    }
}