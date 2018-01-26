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
using Microsoft.Rest;
using Org.Apache.REEF.Tang.Annotations;
using Microsoft.Rest.Azure.Authentication;
using Org.Apache.REEF.IO.FileSystem.AzureDataLake.Parameters;

namespace Org.Apache.REEF.IO.FileSystem.AzureDataLake
{
    internal sealed class SecretKeyAdlsCredentials : IAdlsCredentials
    {
        public ServiceClientCredentials Credentials { get; }

        [Inject]
        private SecretKeyAdlsCredentials([Parameter(typeof(Tenant))] string tenant,
            [Parameter(typeof(TokenAudience))] string tokenAudience,
            [Parameter(typeof(ClientId))] string clientId,
            [Parameter(typeof(SecretKey))] string secretKey)
        {
            SynchronizationContext.SetSynchronizationContext(new SynchronizationContext());

            var serviceSettings = ActiveDirectoryServiceSettings.Azure;
            serviceSettings.TokenAudience = new Uri(tokenAudience);

            Credentials = ApplicationTokenProvider.LoginSilentAsync(tenant, clientId, secretKey, serviceSettings).Result;
        }
    }
}
