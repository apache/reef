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
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.AsyncUtils;

namespace Org.Apache.REEF.Client.YARN.RestClient
{
    /// <summary>
    /// Interface for the client that executes the RestRequests and handles
    /// errors and retries
    /// </summary>
    [DefaultImplementation(typeof(RestClient))]
    internal interface IRestClient
    {
        /// <summary>
        /// Execute request where the response is expected to be type T
        /// </summary>
        Task<RestResponse<T>> ExecuteRequestAsync<T>(RestRequest request, Uri requestBaseUri, CancellationToken cancellationToken);

        /// <summary>
        /// Execute request where no response object is expected
        /// </summary>
        Task<RestResponse<VoidResult>> ExecuteRequestAsync(RestRequest request, Uri requestBaseUri, CancellationToken cancellationToken);
    }
}