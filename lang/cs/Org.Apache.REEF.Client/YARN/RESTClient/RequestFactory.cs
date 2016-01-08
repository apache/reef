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

using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.YARN.RestClient
{
    /// <summary>
    /// Factory to generate REST requests
    /// </summary>
    internal class RequestFactory : IRequestFactory
    {
        private readonly ISerializer _serializer;
        private readonly string _baseResourceString;

        [Inject]
        private RequestFactory(ISerializer serializer)
        {
            _serializer = serializer;
            _baseResourceString = @"ws/v1/";
        }

        /// <summary>
        /// Generate REST request
        /// </summary>
        public RestRequest CreateRestRequest(string resourcePath, Method method)
        {
            return CreateRestRequest(resourcePath, method, null);
        }

        /// <summary>
        /// Generate REST request
        /// </summary>
        public RestRequest CreateRestRequest(string resourcePath, Method method, string rootElement)
        {
            return CreateRestRequest(resourcePath, method, rootElement, null);
        }

        /// <summary>
        /// Generate REST request
        /// </summary>
        public RestRequest CreateRestRequest(string resourcePath, Method method, string rootElement, object body)
        {
            var request = new RestRequest
            {
                Resource = _baseResourceString + resourcePath,
                RootElement = rootElement,
                Method = method,
            };

            if (body != null)
            {
                string content = _serializer.Serialize(body);
                request.AddBody(content);
            }

            return request;
        }
    }
}