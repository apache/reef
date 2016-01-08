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
using Org.Apache.REEF.Client.YARN.RestClient;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.AsyncUtils;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Client.Yarn.RestClient
{
    /// <summary>
    /// Executes a REST request
    /// </summary>
    internal class RestRequestExecutor : IRestRequestExecutor
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(RestRequestExecutor));
        private readonly IRestClient _client;

        [Inject]
        private RestRequestExecutor(
            IRestClient client)
        {
            _client = client;
        }

        public async Task<T> ExecuteAsync<T>(
            RestRequest request,
            Uri requestUri,
            CancellationToken cancellationToken)
        {
            RestResponse<T> response;
            try
            {
                response = await _client.ExecuteRequestAsync<T>(request, requestUri, cancellationToken);
            }
            catch (Exception exception)
            {
                throw new YarnRestAPIException("Unhandled exception in executing REST request.", exception);
            }

            HandleResponse(response.Exception, response.StatusCode, response.Content);

            return response.Data;
        }

        public async Task<RestResponse<VoidResult>> ExecuteAsync(RestRequest request,
            Uri requestUri,
            CancellationToken cancellationToken)
        {
            RestResponse<VoidResult> response;
            try
            {
                response = await _client.ExecuteRequestAsync(request, requestUri, cancellationToken);
            }
            catch (Exception exception)
            {
                throw new YarnRestAPIException("Unhandled exception in executing REST request.", exception);
            }

            HandleResponse(response.Exception, response.StatusCode, response.Content);
            return response;
        }

        private static void HandleResponse(Exception responseException, HttpStatusCode httpStatusCode, string content)
        {
            if (responseException != null)
            {
                throw new YarnRestAPIException("Executing REST API failed", responseException);
            }

            // HTTP status code greater than 300 is unexpected here.
            // See if the server sent a error response and throw suitable
            // exception to user.
            if (httpStatusCode >= HttpStatusCode.Ambiguous)
            {
                Log.Log(Level.Error,
                    "RESTRequest failed. StatusCode: {0}; Response: {1}",
                    httpStatusCode,
                    content);
                Error errorResponse;
                try
                {
                    errorResponse = JsonConvert.DeserializeObject<Error>(content);
                }
                catch (Exception exception)
                {
                    throw new YarnRestAPIException("Unhandled exception in deserializing error response.", exception);
                }

                throw new YarnRestAPIException { Error = errorResponse };
            }
        }
    }
}