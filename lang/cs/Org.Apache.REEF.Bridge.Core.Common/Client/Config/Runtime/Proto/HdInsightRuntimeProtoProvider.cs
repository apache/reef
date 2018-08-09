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
using Org.Apache.REEF.Bridge.Core.Proto;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Bridge.Core.Common.Client.Config.Runtime.Proto
{
    internal sealed class HdInsightRuntimeProtoProvider : IRuntimeProtoProvider
    {
        private readonly HDIRuntimeParameters _hdiRuntimeParameters;

        [Inject]
        private HdInsightRuntimeProtoProvider(
            [Parameter(Value = typeof(HdInsightRuntimeParameters.HdInsightUnsafeDeployment))] bool unsafeDeplyment,
            [Parameter(Value = typeof(HdInsightRuntimeParameters.HdInsightUserName))] string userName,
            [Parameter(Value = typeof(HdInsightRuntimeParameters.HdInsightPassword))] string password,
            [Parameter(Value = typeof(HdInsightRuntimeParameters.HdInsightUrl))] string url,
            [Parameter(Value = typeof(HdInsightRuntimeParameters.AzureStorageAccountKey))] string storageAccountKey,
            [Parameter(Value = typeof(HdInsightRuntimeParameters.AzureStorageAccountName))] string storageAccountName,
            [Parameter(Value = typeof(HdInsightRuntimeParameters.AzureStorageContainerName))] string storageContainerName)
        {
            _hdiRuntimeParameters = new HDIRuntimeParameters()
            {
                Unsafe = unsafeDeplyment,
                HdiUserName = userName,
                HdiPassword = password,
                HdiUrl = url,
                AzureStorageAccountKey = storageAccountKey,
                AzureStorageAccountName = storageAccountName,
                AzureStorageContainerName = storageContainerName
            };
        }

        public void SetParameters(DriverClientConfiguration driverClientConfiguration)
        {
            driverClientConfiguration.HdiRuntime = _hdiRuntimeParameters;
        }
    }
}