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

using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IO.FileSystem.AzureBlob.RetryPolicy.Exponential
{
    public sealed class ExponentialRetryPolicyConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        /// The exponential retry count.
        /// </summary>
        public static readonly OptionalParameter<int> RetryCount = new OptionalParameter<int>();

        /// <summary>
        /// The exponential retry interval in seconds.
        /// </summary>
        public static readonly OptionalParameter<double> RetryInterval = new OptionalParameter<double>();

        /// <summary>
        /// Configuration Module for ExponentialRetryPolicy implementation of IAzureBlobRetryPolicy.
        /// </summary>
        public static ConfigurationModule ConfigurationModule = new ExponentialRetryPolicyConfiguration()
            .BindImplementation(GenericType<IAzureBlobRetryPolicy>.Class, GenericType<ExponentialRetryPolicy>.Class)
            .BindNamedParameter(GenericType<ExponentialRetryPolicyParameterNames.RetryCount>.Class, RetryCount)
            .BindNamedParameter(GenericType<ExponentialRetryPolicyParameterNames.RetryInterval>.Class, RetryInterval)
            .Build();
    }
}