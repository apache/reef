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
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IO.FileSystem.AzureBlob.RetryPolicy.Exponential
{
    /// <summary>
    /// Represents a retry policy that performs a specified number of retries,
    /// using a randomized exponential back off scheme to determine the interval between retries.
    /// </summary>
    internal sealed class ExponentialRetryPolicy : IAzureBlobRetryPolicy
    {
        private readonly IRetryPolicy _retryPolicy;

        [Inject]
        private ExponentialRetryPolicy(
            [Parameter(typeof(ExponentialRetryPolicyParameterNames.RetryCount))] int retryCount,
            [Parameter(typeof(ExponentialRetryPolicyParameterNames.RetryInterval))] double retryInterval)
        {
            _retryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(retryInterval), retryCount);
        }

        public IRetryPolicy CreateInstance()
        {
            return _retryPolicy;
        }

        public bool ShouldRetry(int currentRetryCount, int statusCode, Exception lastException, out TimeSpan retryInterval,
            OperationContext operationContext)
        {
            return _retryPolicy.ShouldRetry(currentRetryCount, statusCode, lastException, out retryInterval, operationContext);

        }
    }
}