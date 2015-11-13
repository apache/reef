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
using Org.Apache.REEF.IO.FileSystem.AzureBlob.Parameters;
using Org.Apache.REEF.IO.FileSystem.AzureBlob.RetryPolicy;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.IO.FileSystem.AzureBlob
{
    /// <summary>
    /// Configuration Module for the Azure Block Blob (WASB) file system implementation of IFileSystem.
    /// </summary>
    /// <remarks>
    /// Stream-based operations are not supported.
    /// </remarks>
    public sealed class AzureBlockBlobFileSystemConfiguration : ConfigurationModuleBuilder
    {
        public static readonly RequiredParameter<string> ConnectionString = new RequiredParameter<string>();

        public static readonly OptionalImpl<IAzureBlobRetryPolicy> RetryPolicy = new OptionalImpl<IAzureBlobRetryPolicy>(); 

        /// <summary>
        /// Set AzureBlockBlobFileSystemProvider to DriverConfigurationProviders.
        /// Set all the parameters needed for injecting AzureBlockBlobFileSystemProvider.
        /// </summary>
        public static readonly ConfigurationModule ConfigurationModule = new AzureBlockBlobFileSystemConfiguration()
            .BindSetEntry<DriverConfigurationProviders, AzureBlockBlobFileSystemConfigurationProvider, IConfigurationProvider>(
                GenericType<DriverConfigurationProviders>.Class, GenericType<AzureBlockBlobFileSystemConfigurationProvider>.Class)
            .BindImplementation(GenericType<IFileSystem>.Class, GenericType<AzureBlockBlobFileSystem>.Class)
            .BindNamedParameter(GenericType<AzureStorageConnectionString>.Class, ConnectionString)
            .BindImplementation(GenericType<IAzureBlobRetryPolicy>.Class, RetryPolicy)
            .Build();
    }
}
