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

using Microsoft.Azure.Batch;
using Org.Apache.REEF.Client.AzureBatch.Parameters;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Client.DotNet.AzureBatch
{
    public sealed class ContainerRegistryProvider
    {
        public string ContainerRegistryServer { get; }
        public string ContainerRegistryUsername { get; }
        public string ContainerRegistryPassword { get; }
        public string ContainerImageName { get; }

        [Inject]
        public ContainerRegistryProvider(
            [Parameter(typeof(ContainerRegistryServer))] string containerRegistryServer,
            [Parameter(typeof(ContainerRegistryUsername))] string containerRegistryUsername,
            [Parameter(typeof(ContainerRegistryPassword))] string containerRegistryPassword,
            [Parameter(typeof(ContainerImageName))] string containerImageName
      )
        {
            ContainerRegistryServer = containerRegistryServer;
            ContainerRegistryUsername = containerRegistryUsername;
            ContainerRegistryPassword = containerRegistryPassword;
            ContainerImageName = containerImageName;
        }

        public bool IsValid()
        {
            return !string.IsNullOrEmpty(ContainerRegistryServer);
        }

        public ContainerRegistry GetContainerRegistry()
        {
            if (!IsValid())
            {
                return null;
            }

            return new ContainerRegistry(
                userName: ContainerRegistryUsername,
                registryServer: ContainerRegistryServer,
                password: ContainerRegistryPassword);
        }
    }
}
