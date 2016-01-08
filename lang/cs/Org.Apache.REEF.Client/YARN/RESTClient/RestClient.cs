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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.AsyncUtils;

namespace Org.Apache.REEF.Client.YARN.RestClient
{
    /// <summary>
    /// Implementation of RestClient which uses HTTPClient to 
    /// make REST requests and handles errors
    /// </summary>
    internal class RestClient : IRestClient
    {
        private readonly IDeserializer _deserializer;
        private readonly IHttpClient _httpClient;

        [Inject]
        private RestClient(IDeserializer deserializer, IHttpClient httpClient)
        {
            _httpClient = httpClient;
            _deserializer = deserializer;
        }

        /// <summary>
        /// Execute request where no response object is expected
        /// </summary>
        public async Task<RestResponse<VoidResult>> ExecuteRequestAsync(
            RestRequest request,
            Uri requestBaseUri,
            CancellationToken cancellationToken)
        {
            var httpResponseMessage = await GetHttpResponseAsync(request, requestBaseUri, cancellationToken);
            Exception exception = null;
            if (!httpResponseMessage.IsSuccessStatusCode)
            {
                exception =
                    new HttpRequestException(string.Format("HTTP call failed with status [{0}] and content [{1}]",
                        httpResponseMessage.StatusCode,
                        await GetContentStringFromHttpResponseMessage(httpResponseMessage)));
            }

            return new RestResponse<VoidResult>
            {
                Data = new VoidResult(),
                Exception = exception,
                StatusCode = httpResponseMessage.StatusCode
            };
        }

        /// <summary>
        /// Execute request where the response is expected to be type T
        /// </summary>
        public async Task<RestResponse<T>> ExecuteRequestAsync<T>(
            RestRequest request,
            Uri requestBaseUri,
            CancellationToken cancellationToken)
        {
            var httpResponseMessage = await GetHttpResponseAsync(request, requestBaseUri, cancellationToken);
            string contentString = await GetContentStringFromHttpResponseMessage(httpResponseMessage);
            Exception exception = null;
            T returnObj = default(T);

            if (!httpResponseMessage.IsSuccessStatusCode)
            {
                exception =
                    new HttpRequestException(string.Format("HTTP call failed with status [{0}] and content [{1}]",
                        httpResponseMessage.StatusCode,
                        contentString));
            }
            else
            {
                returnObj = _deserializer.Deserialize<T>(contentString,
                    request.RootElement);
            }
            return new RestResponse<T>
            {
                Content = contentString,
                Data = returnObj,
                StatusCode = httpResponseMessage.StatusCode,
                Exception = exception
            };
        }

        private async Task<HttpResponseMessage> GetHttpResponseAsync(
            RestRequest request,
            Uri requestBaseUri,
            CancellationToken cancellationToken)
        {
            HttpResponseMessage httpResponseMessage;
            var requestResource = requestBaseUri + request.Resource;
            switch (request.Method)
            {
                case Method.GET:
                    httpResponseMessage = await _httpClient.GetAsync(
                        requestResource,
                        cancellationToken);
                    break;
                case Method.PUT:
                    httpResponseMessage = await _httpClient.PutAsync(
                        requestResource,
                        request.Content,
                        cancellationToken);
                    break;
                case Method.POST:
                    httpResponseMessage = await _httpClient.PostAsync(
                        requestResource,
                        request.Content,
                        cancellationToken);
                    break;
                default:
                    throw new InvalidOperationException(string.Format("Unknown method type {0}", request.Method));
            }

            return httpResponseMessage;
        }

        private static async Task<string> GetContentStringFromHttpResponseMessage(HttpResponseMessage response)
        {
            return response.Content == null ? string.Empty : await response.Content.ReadAsStringAsync();
        }
    }
}