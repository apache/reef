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

using Microsoft.Azure.DataLake.Store;
using Org.Apache.REEF.IO.FileSystem.AzureDataLake.Parameters;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.IO.FileSystem.AzureDataLake
{
    /// <summary>
    /// A proxy class for AdlsClient, mainly in order to fake for unit testing.
    /// </summary>
    internal sealed class AzureDataLakeStoreClient : IDataLakeStoreClient
    {
        private readonly AdlsClient _adlsClient;

        [Inject]
        private AzureDataLakeStoreClient([Parameter(typeof(DataLakeStorageAccountFQDN))] string adlsAccountFQDN, IAdlsCredentials adlsCredentials)
        {
            _adlsClient = AdlsClient.CreateClient(adlsAccountFQDN, adlsCredentials.Credentials);
        }

        public AdlsClient GetAdlsClient()
        {
            return _adlsClient;
        }

        public string AccountFQDN => _adlsClient.AccountFQDN;
    }
}
