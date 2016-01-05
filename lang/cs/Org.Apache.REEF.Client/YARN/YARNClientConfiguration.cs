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

using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.YARN;
using Org.Apache.REEF.Client.YARN.Parameters;
using Org.Apache.REEF.Client.YARN.RestClient;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Client.Yarn
{
    /// <summary>
    /// The Configuration for the YARN Client
    /// </summary>
    public sealed class YARNClientConfiguration : ConfigurationModuleBuilder
    {
        public static readonly OptionalParameter<string> JobSubmissionFolderPrefix = new OptionalParameter<string>();
        public static readonly OptionalParameter<string> SecurityTokenKind = new OptionalParameter<string>();
        public static readonly OptionalParameter<string> SecurityTokenService = new OptionalParameter<string>();
        public static readonly OptionalImpl<IYarnRestClientCredential> YarnRestClientCredential = new OptionalImpl<IYarnRestClientCredential>();

        public static ConfigurationModule ConfigurationModule = new YARNClientConfiguration()
            .BindImplementation(GenericType<IREEFClient>.Class, GenericType<YarnREEFClient>.Class)
            .BindNamedParameter(GenericType<JobSubmissionDirectoryPrefixParameter>.Class, JobSubmissionFolderPrefix)
            .BindNamedParameter(GenericType<SecurityTokenKindParameter>.Class, SecurityTokenKind)
            .BindNamedParameter(GenericType<SecurityTokenServiceParameter>.Class, SecurityTokenService)
            .Build();

        [Unstable("This is temporary configuration until REEF-70 is completed when ConfigurationModule" +
                  " and ConfigurationModuleYARNRest would be merged.")]
        public static ConfigurationModule ConfigurationModuleYARNRest = new YARNClientConfiguration()
            .BindImplementation(GenericType<IREEFClient>.Class, GenericType<YarnREEFDotNetClient>.Class)
            .BindImplementation(GenericType<IYarnRestClientCredential>.Class, YarnRestClientCredential)
            .BindNamedParameter(GenericType<JobSubmissionDirectoryPrefixParameter>.Class, JobSubmissionFolderPrefix)
            .BindNamedParameter(GenericType<SecurityTokenKindParameter>.Class, SecurityTokenKind)
            .BindNamedParameter(GenericType<SecurityTokenServiceParameter>.Class, SecurityTokenService)
            .Build();
    }
}