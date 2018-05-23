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

using Org.Apache.REEF.Bridge.Core.Common.Client;
using Org.Apache.REEF.Bridge.Core.Common.Client.Config;
using Org.Apache.REEF.Bridge.Core.Common.Client.Default;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;

namespace Org.Apache.REEF.Bridge.Core.Grpc.Client
{
    /// <summary>
    /// Factory class for creating a ClientLauncher
    /// </summary>
    public static class ClientLauncherFactory
    {
        /// <summary>
        /// Get a client launcher that does not include a client configuration, which
        /// will default to all default client handlers e.g., <see cref="DefaultRunningJobHandler"/>
        /// </summary>
        /// <param name="runtimeConfiguration">One of YARN, Local, Azure Batch, etc. runtime configuration</param>
        /// <param name="driverRuntimeConfiguration">The core driver runtime configuration <see cref="DriverRuntimeConfiguration"/></param>
        /// <returns>ClientLauncher</returns>
        public static IClientLauncher GetLauncher(
            IConfiguration runtimeConfiguration,
            IConfiguration driverRuntimeConfiguration)
        {
            return GetLauncher(runtimeConfiguration, driverRuntimeConfiguration, Optional<IConfiguration>.Empty());
        }

        /// <summary>
        /// Get a client launcher that optionally includes a client configuration.
        /// </summary>
        /// <param name="runtimeConfiguration">One of YARN, Local, Azure Batch, etc. runtime configuration</param>
        /// <param name="driverRuntimeConfiguration">The core driver runtime configuration <see cref="DriverRuntimeConfiguration"/></param>
        /// <param name="clientConfiguration">Client configuration <see cref="ClientConfiguration"/></param>
        /// <returns></returns>
        public static IClientLauncher GetLauncher(
            IConfiguration runtimeConfiguration,
            IConfiguration driverRuntimeConfiguration,
            Optional<IConfiguration> clientConfiguration)
        {
            return TangFactory.GetTang()
                .NewInjector(Configurations.Merge(
                    runtimeConfiguration,
                    driverRuntimeConfiguration,
                    clientConfiguration.OrElse(TangFactory.GetTang().NewConfigurationBuilder().Build())))
                .GetInstance<ClientLauncher>();
        }
    }
}
