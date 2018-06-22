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

using Org.Apache.REEF.Common.Evaluator.Parameters;
using Org.Apache.REEF.IO.FileSystem.AzureDataLake.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IO.FileSystem.AzureDataLake
{
    /// <summary>
    /// This provider provides configuration for AzureDataLakeFileSystem
    /// The client that is going to use AzureDataLakeFileSystem in its driver and evaluators should set 
    /// configuration data through AzureDataLakeFileSystemConfiguration module in the client's configuration
    /// </summary>
    internal sealed class AzureDataLakeFileSystemConfigurationProvider : IConfigurationProvider
    {
        private readonly IConfiguration _configuration;

        [Inject]
        private AzureDataLakeFileSystemConfigurationProvider(
            [Parameter(typeof(DataLakeStorageAccountFqdn))] string adlsAccountFqdn,
            [Parameter(typeof(Tenant))] string tenant,
            [Parameter(typeof(ClientId))] string clientId,
            [Parameter(typeof(SecretKey))] string secretKey,
            [Parameter(typeof(TokenAudience))] string tokenAudience)
        {
            _configuration = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IFileSystem>.Class, GenericType<AzureDataLakeFileSystem>.Class)
                .BindImplementation(GenericType<IAdlsCredentials>.Class, GenericType<SecretKeyAdlsCredentials>.Class)
                .BindStringNamedParam<DataLakeStorageAccountFqdn>(adlsAccountFqdn)
                .BindStringNamedParam<Tenant>(tenant)
                .BindStringNamedParam<ClientId>(clientId)
                .BindStringNamedParam<SecretKey>(secretKey)
                .BindStringNamedParam<TokenAudience>(tokenAudience)
                .BindSetEntry<EvaluatorConfigurationProviders, AzureDataLakeFileSystemConfigurationProvider, IConfigurationProvider>()
                .Build();
        }

        public IConfiguration GetConfiguration()
        {
            return _configuration;
        }
    }
}
