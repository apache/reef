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

using Org.Apache.REEF.Common.Client.Parameters;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.IO.FileSystem.AzureDataLake.Parameters;

namespace Org.Apache.REEF.IO.FileSystem.AzureDataLake
{
    /// <summary>
    /// Configuration Module for the Azure Data Lake (ADL) file system implementation of IFileSystem.
    /// </summary>
    public sealed class AzureDataLakeFileSystemConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        /// The account FQDN to be used to connect to the data lake store
        /// </summary>
        public static readonly RequiredParameter<string> DataLakeStorageAccountName = new RequiredParameter<string>();

        /// <summary>
        /// The Tenant to be used to authenticate with Azure
        /// </summary>
        public static readonly RequiredParameter<string> Tenant = new RequiredParameter<string>();

        /// <summary>
        /// The Application ID to be used to authenticate with Azure
        /// </summary>
        public static readonly RequiredParameter<string> ClientId = new RequiredParameter<string>();

        /// <summary>
        /// The Client Secret to be used to authenticate with Azure
        /// </summary>
        public static readonly RequiredParameter<string> SecretKey = new RequiredParameter<string>();

        /// <summary>
        /// The ADL TokenAudience Uri to be used to authenticate with Data Lake Store
        /// </summary>
        public static readonly OptionalParameter<string> TokenAudience = new OptionalParameter<string>();

        /// <summary>
        /// Set AzureDataLakeFileSystemConfigurationProvider to DriverConfigurationProviders.
        /// Set all the parameters needed for injecting AzureDataLakeFileSystemConfigurationProvider.
        /// </summary>
        public static readonly ConfigurationModule ConfigurationModule = new AzureDataLakeFileSystemConfiguration()
            .BindSetEntry<DriverConfigurationProviders, AzureDataLakeFileSystemConfigurationProvider, IConfigurationProvider>(
                GenericType<DriverConfigurationProviders>.Class, GenericType<AzureDataLakeFileSystemConfigurationProvider>.Class)
            .BindImplementation(GenericType<IFileSystem>.Class, GenericType<AzureDataLakeFileSystem>.Class)
            .BindNamedParameter(GenericType<DataLakeStorageAccountName>.Class, DataLakeStorageAccountName)
            .BindNamedParameter(GenericType<Tenant>.Class, Tenant)
            .BindNamedParameter(GenericType<ClientId>.Class, ClientId)
            .BindNamedParameter(GenericType<SecretKey>.Class, SecretKey)
            .BindNamedParameter(GenericType<TokenAudience>.Class, TokenAudience)
            .Build();
    }
}
