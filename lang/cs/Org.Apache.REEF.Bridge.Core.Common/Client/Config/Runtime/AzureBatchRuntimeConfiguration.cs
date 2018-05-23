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
    public sealed class AzureBatchRuntimeConfiguration : ConfigurationModuleBuilder
    {
        private static readonly string AzBatchConfigurationFileEnvironmentVariable = "REEF_AZBATCH_CONF";

        /// <summary>
        /// The Azure Batch account URI.
        /// </summary>
        public static readonly RequiredParameter<string> AzureBatchAccountUri = new RequiredParameter<string>();

        /// <summary>
        /// The Azure Batch account name.
        /// </summary>
        public static readonly RequiredParameter<string> AzureBatchAccountName = new RequiredParameter<string>();

        /// <summary>
        /// The Azure Batch account key.
        /// </summary>
        public static readonly RequiredParameter<string> AzureBatchAccountKey = new RequiredParameter<string>();

        /// <summary>
        /// The Azure Batch pool ID.
        /// </summary>
        public static readonly RequiredParameter<string> AzureBatchPoolId = new RequiredParameter<string>();

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

        public static ConfigurationModule ConfigurationModule = new AzureBatchRuntimeConfiguration()
            .BindImplementation(GenericType<IRuntimeProtoProvider>.Class, GenericType<AzureBatchRuntimeProtoProvider>.Class)
            .BindNamedParameter(GenericType<AzureBatchRuntimeParameters.AzureBatchAccountUri>.Class,
                AzureBatchAccountUri)
            .BindNamedParameter(GenericType<AzureBatchRuntimeParameters.AzureBatchAccountName>.Class,
                AzureBatchAccountName)
            .BindNamedParameter(GenericType<AzureBatchRuntimeParameters.AzureBatchAccountKey>.Class,
                AzureBatchAccountKey)
            .BindNamedParameter(GenericType<AzureBatchRuntimeParameters.AzureBatchPoolId>.Class, AzureBatchPoolId)
            .BindNamedParameter(GenericType<AzureBatchRuntimeParameters.AzureStorageAccountName>.Class,
                AzureStorageAccountName)
            .BindNamedParameter(GenericType<AzureBatchRuntimeParameters.AzureStorageAccountKey>.Class,
                AzureStorageAccountKey)
            .BindNamedParameter(GenericType<AzureBatchRuntimeParameters.AzureStorageContainerName>.Class,
                AzureStorageContainerName)
            .Build();

        public static IConfiguration FromTextFile(string file)
        {
            return new AvroConfigurationSerializer().FromFile(file);
        }

        public static IConfiguration FromEnvironment()
        {
            var configurationPath = Environment.GetEnvironmentVariable(AzBatchConfigurationFileEnvironmentVariable);
            if (configurationPath == null)
            {
                throw new ArgumentException($"Environment Variable {AzBatchConfigurationFileEnvironmentVariable} not set");
            }

            if (!File.Exists(configurationPath))
            {
                throw new ArgumentException($"File located by Environment Variable {AzBatchConfigurationFileEnvironmentVariable} cannot be read.");
            }
            return FromTextFile(configurationPath);
        }
    }
}
