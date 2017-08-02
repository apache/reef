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

using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.YARN.RestClient
{
    /// <summary>
    /// Pass through HTTP client which calls into <see cref="System.Net.Http.HttpClient"/>
    /// </summary>
    internal class HttpClient : IHttpClient
    {
        private readonly System.Net.Http.HttpClient _httpClient;

        [Inject]
        private HttpClient(IYarnRestClientCredential yarnRestClientCredential)
        {
            _httpClient = new System.Net.Http.HttpClient(
                new HttpClientRetryHandler(new HttpClientHandler { Credentials = yarnRestClientCredential.Credentials }),
                disposeHandler: false);
        }

        public async Task<HttpResponseMessage> GetAsync(string requestResource, CancellationToken cancellationToken)
        {
            return await _httpClient.GetAsync(requestResource, cancellationToken);
        }

        public async Task<HttpResponseMessage> PostAsync(string requestResource,
            StringContent content,
            CancellationToken cancellationToken)
        {
            return await _httpClient.PostAsync(requestResource, content, cancellationToken);
        }

        public async Task<HttpResponseMessage> PutAsync(string requestResource,
            StringContent content,
            CancellationToken cancellationToken)
        {
            return await _httpClient.PutAsync(requestResource, content, cancellationToken);
        }
    }
}