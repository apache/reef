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
using Org.Apache.REEF.Client.API.Parameters;
using Org.Apache.REEF.Client.AzureBatch.Parameters;
using Org.Apache.REEF.Client.DotNet.AzureBatch;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Parameters;
using System;
using System.Collections.Generic;
using System.IO;

namespace Org.Apache.REEF.Client.AzureBatch
{
    /// <summary>
    /// The Configuration for the Azure Batch Client
    /// </summary>
    public sealed class AzureBatchRuntimeClientConfiguration : ConfigurationModuleBuilder
    {
        public static readonly string AzBatchConfigurationFileEnvironmentVariable = "REEF_AZBATCH_CONF";

        public static readonly RequiredParameter<string> AzureBatchAccountUri = new RequiredParameter<string>();
        public static readonly RequiredParameter<string> AzureBatchAccountName = new RequiredParameter<string>();
        public static readonly RequiredParameter<string> AzureBatchAccountKey = new RequiredParameter<string>();
        public static readonly RequiredParameter<string> AzureBatchPoolId = new RequiredParameter<string>();

        public static readonly RequiredParameter<string> AzureStorageAccountName = new RequiredParameter<string>();
        public static readonly RequiredParameter<string> AzureStorageAccountKey = new RequiredParameter<string>();
        public static readonly RequiredParameter<string> AzureStorageContainerName = new RequiredParameter<string>();

        public static readonly OptionalParameter<int> DriverHTTPConnectionRetryInterval = new OptionalParameter<int>();
        public static readonly OptionalParameter<int> DriverHTTPConnectionAttempts = new OptionalParameter<int>();

        public static readonly OptionalParameter<IList<string>> AzureBatchPoolDriverPortsList = new OptionalParameter<IList<string>>();

        public static readonly OptionalParameter<string> ContainerRegistryServer = new OptionalParameter<string>();
        public static readonly OptionalParameter<string> ContainerRegistryUsername = new OptionalParameter<string>();
        public static readonly OptionalParameter<string> ContainerRegistryPassword = new OptionalParameter<string>();
        public static readonly OptionalParameter<string> ContainerImageName = new OptionalParameter<string>();

        public static ConfigurationModule ConfigurationModule = new AzureBatchRuntimeClientConfiguration()
            .BindImplementation(GenericType<IREEFClient>.Class, GenericType<AzureBatchDotNetClient>.Class)
            .BindNamedParameter(GenericType<AzureBatchAccountUri>.Class, AzureBatchAccountUri)
            .BindNamedParameter(GenericType<AzureBatchAccountName>.Class, AzureBatchAccountName)
            .BindNamedParameter(GenericType<AzureBatchAccountKey>.Class, AzureBatchAccountKey)
            .BindNamedParameter(GenericType<AzureBatchPoolId>.Class, AzureBatchPoolId)
            .BindNamedParameter(GenericType<AzureStorageAccountName>.Class, AzureStorageAccountName)
            .BindNamedParameter(GenericType<AzureStorageAccountKey>.Class, AzureStorageAccountKey)
            .BindNamedParameter(GenericType<AzureStorageContainerName>.Class, AzureStorageContainerName)
            .BindNamedParameter(GenericType<DriverHTTPConnectionRetryInterval>.Class, DriverHTTPConnectionRetryInterval)
            .BindNamedParameter(GenericType<DriverHTTPConnectionAttempts>.Class, DriverHTTPConnectionAttempts)
            .BindNamedParameter(GenericType<AzureBatchPoolDriverPortsList>.Class, AzureBatchPoolDriverPortsList)
            .BindNamedParameter(GenericType<ContainerRegistryServer>.Class, ContainerRegistryServer)
            .BindNamedParameter(GenericType<ContainerRegistryUsername>.Class, ContainerRegistryUsername)
            .BindNamedParameter(GenericType<ContainerRegistryPassword>.Class, ContainerRegistryPassword)
            .BindNamedParameter(GenericType<ContainerImageName>.Class, ContainerImageName)
            .Build();

        public static IConfiguration FromTextFile(string file)
        {
            return new AvroConfigurationSerializer().FromFile(file);
        }

        public static ConfigurationModule GetConfigurationModule(List<string> ports)
        {
            ConfigurationModuleBuilder moduleBuilder = AzureBatchRuntimeClientConfiguration.ConfigurationModule.Builder;

            foreach (string port in ports)
            {
                moduleBuilder = moduleBuilder.BindSetEntry<TcpPortSet, int>(GenericType<TcpPortSet>.Class, port);
            }

            return moduleBuilder.Build();
        }

        public static IConfiguration FromEnvironment()
        {
            string configurationPath = Environment.GetEnvironmentVariable(AzBatchConfigurationFileEnvironmentVariable);

            if (configurationPath == null)
            {
                throw new ArgumentException(@"Environment Variable {AzureBatchClientConfiguration.AzBatchConfigurationFileEnvironmentVariable} not set");
            }

            if (!File.Exists(configurationPath))
            {
                throw new ArgumentException(@"File located by Environment Variable {AzureBatchClientConfiguration.AzBatchConfigurationFileEnvironmentVariable} cannot be read.");
            }

            return FromTextFile(configurationPath);
        }
    }
}
