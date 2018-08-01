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
using System.IO;
using Org.Apache.REEF.Bridge.Core.Common.Client.Config.Runtime.Proto;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Bridge.Core.Common.Client.Config.Runtime
{
    public sealed class HdInsightRuntimeConfiguration : ConfigurationModuleBuilder
    {
        private static readonly string HdiConfigurationFileEnvironmentVariable = "REEF_HDI_CONF";

        /// <summary>
        /// HDInsight URL.
        /// </summary>
        public static readonly RequiredParameter<string> HdInsightUrl = new RequiredParameter<string>();

        /// <summary>
        /// The user name.
        /// </summary>
        public static readonly RequiredParameter<string> HdInsightUserName = new RequiredParameter<string>();

        /// <summary>
        /// Password.
        /// </summary>
        public static readonly RequiredParameter<string> HdInsightPassword = new RequiredParameter<string>();

        /// <summary>
        /// The Azure Storage account name.
        /// </summary>
        public static readonly RequiredParameter<string> AzureStorageAccountName = new RequiredParameter<string>();

        /// <summary>
        /// The Azure Storage account key.
        /// </summary>
        public static readonly RequiredParameter<string> AzureStorageAccountKey = new RequiredParameter<string>();

        /// <summary>
        /// The Azure Storage container name.
        /// </summary>
        public static readonly RequiredParameter<string> AzureStorageContainerName = new RequiredParameter<string>();

        /// <summary>
        /// Indicates whether to target an unsafe deployment.
        /// </summary>
        public static readonly OptionalParameter<bool> HdInsightUnsafeDeployment = new OptionalParameter<bool>();

        public static ConfigurationModule ConfigurationModule = new HdInsightRuntimeConfiguration()
            .BindImplementation(GenericType<IRuntimeProtoProvider>.Class, GenericType<HdInsightRuntimeProtoProvider>.Class)
            .BindNamedParameter(GenericType<HdInsightRuntimeParameters.HdInsightUrl>.Class, HdInsightUrl)
            .BindNamedParameter(GenericType<HdInsightRuntimeParameters.HdInsightUserName>.Class, HdInsightUserName)
            .BindNamedParameter(GenericType<HdInsightRuntimeParameters.HdInsightPassword>.Class, HdInsightPassword)
            .BindNamedParameter(GenericType<HdInsightRuntimeParameters.AzureStorageAccountName>.Class,
                AzureStorageAccountName)
            .BindNamedParameter(GenericType<HdInsightRuntimeParameters.AzureStorageAccountKey>.Class,
                AzureStorageAccountKey)
            .BindNamedParameter(GenericType<HdInsightRuntimeParameters.AzureStorageContainerName>.Class,
                AzureStorageContainerName)
            .BindNamedParameter(GenericType<HdInsightRuntimeParameters.HdInsightUnsafeDeployment>.Class, HdInsightUnsafeDeployment)
            .Build();

        public static IConfiguration FromTextFile(string file)
        {
            return new AvroConfigurationSerializer().FromFile(file);
        }

        public static IConfiguration FromEnvironment()
        {
            var configurationPath = Environment.GetEnvironmentVariable(HdiConfigurationFileEnvironmentVariable);
            if (configurationPath == null)
            {
                throw new ArgumentException($"Environment Variable {HdiConfigurationFileEnvironmentVariable} not set");
            }

            if (!File.Exists(configurationPath))
            {
                throw new ArgumentException($"File located by Environment Variable {HdiConfigurationFileEnvironmentVariable} cannot be read.");
            }
            return FromTextFile(configurationPath);
        }
    }
}
