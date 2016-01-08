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
using Microsoft.Practices.TransientFaultHandling;

namespace Org.Apache.REEF.Client.YARN.RestClient
{
    /// <summary>
    /// DelegatingHandler for retrying requests with HTTP client
    /// </summary>
    internal class HttpClientRetryHandler : DelegatingHandler
    {
        private const int RetryCount = 3;
        private static readonly TimeSpan MinBackoffTimeSpan = TimeSpan.FromMilliseconds(100);
        private static readonly TimeSpan MaxBackoffTimeSpan = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan DeltaBackoffTimeSpan = TimeSpan.FromMilliseconds(500);

        private readonly RetryPolicy<AllErrorsTransientStrategy> _retryPolicy;

        public HttpClientRetryHandler(HttpMessageHandler innerHandler)
            : base(innerHandler)
        {
            this._retryPolicy = new RetryPolicy<AllErrorsTransientStrategy>(
                new ExponentialBackoff(
                    "YarnRESTRetryHandler",
                    RetryCount,
                    MinBackoffTimeSpan,
                    MaxBackoffTimeSpan,
                    DeltaBackoffTimeSpan,
                    firstFastRetry: true));
        }

        protected override async Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            return await this._retryPolicy.ExecuteAsync(
                async () => await base.SendAsync(request, cancellationToken),
                cancellationToken);
        }
    }

    internal class AllErrorsTransientStrategy : ITransientErrorDetectionStrategy
    {
        public bool IsTransient(Exception ex)
        {
            return true;
        }
    }
}