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

using System.Security;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.Yarn.RestClient;
using Org.Apache.REEF.Client.YARN.Parameters;
using Org.Apache.REEF.Client.YARN.RestClient;
using Org.Apache.REEF.IO.FileSystem.AzureBlob;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Client.YARN.HDI
{
    public class HDInsightClientConfiguration : ConfigurationModuleBuilder
    {
        public static readonly RequiredParameter<string> HDInsightUsernameParameter = new RequiredParameter<string>();
        public static readonly RequiredParameter<string> HDInsightUrlParameter = new RequiredParameter<string>();

        // Required because user needs to provide the correct path which includes the container name for their 
        // blob storage account where they want to upload the resource files. This container should be the container that
        // HDInsight is using.
        public static readonly RequiredParameter<string> JobSubmissionDirectoryPrefix = new RequiredParameter<string>();

        public static readonly OptionalParameter<string> HDInsightPasswordParameter = new OptionalParameter<string>();

        public static readonly OptionalParameter<SecureString> HDInsightSecurePasswordParameter =
            new OptionalParameter<SecureString>();

        /// <summary>
        /// Provides <see cref="ConfigurationModule"/> to be used to obtain <see cref="IREEFClient"/> to
        /// submit REEF application to HDInsight.
        /// Sample usage:
        /// <code>
        /// <![CDATA[
        /// const string connectionString = "ConnString";
        /// var config = HDInsightClientConfiguration.GetConfigurationModule(azureBlockBlobConfig)
        ///              .Set(AzureBlockBlobFileSystemConfiguration.ConnectionString, connectionString)
        ///              .Set(HDInsightClientConfiguration.HDInsightUsernameParameter, @"foo")
        ///              .Set(HDInsightClientConfiguration.HDInsightUrlParameter, @"https://bar.azurehdinsight.net/")
        ///              ...;
        /// var client = TangFactory.GetTang().NewInjector(config).GetInstance<IREEFClient>();
        /// ]]>
        /// </code>
        /// </summary>
        public static ConfigurationModule ConfigurationModule = new HDInsightClientConfiguration()
            .BindNamedParameter(GenericType<HDInsightUrl>.Class, HDInsightUrlParameter)
            .BindNamedParameter(GenericType<HDInsightUserName>.Class, HDInsightUsernameParameter)
            .BindNamedParameter(GenericType<HDInsightPasswordSecureString>.Class,
                HDInsightSecurePasswordParameter)
            .BindNamedParameter(GenericType<JobSubmissionDirectoryPrefixParameter>.Class,
                JobSubmissionDirectoryPrefix)
            .BindNamedParameter(GenericType<HDInsightPasswordString>.Class, HDInsightPasswordParameter)
            .BindImplementation(GenericType<IREEFClient>.Class, GenericType<YarnREEFDotNetClient>.Class)
            .BindImplementation(GenericType<IYarnRestClientCredential>.Class,
                GenericType<HDInsightCredential>.Class)
            .BindImplementation(GenericType<IUrlProvider>.Class, GenericType<HDInsightUrlProvider>.Class)
            .BindImplementation(GenericType<IJobResourceUploader>.Class,
                GenericType<FileSystemJobResourceUploader>.Class)
            .BindImplementation(GenericType<IYarnCommandLineEnvironment>.Class,
                GenericType<HDInsightCommandLineEnvironment>.Class)
            .BindImplementation(GenericType<IResourceFileRemoteUrlToClusterUrlConverter>.Class,
                GenericType<HDInsightResourceFileRemoteUrlToClusterUrlConverter>.Class)
            .Merge(AzureBlobFileSystemConfiguration.ConfigurationModule)
            .Build();
    }
}