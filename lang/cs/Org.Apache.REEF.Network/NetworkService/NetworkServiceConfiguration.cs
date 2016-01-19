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
using System.Diagnostics.CodeAnalysis;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.NetworkService
{
    [Obsolete("Deprecated in 0.14.")]
    public class NetworkServiceConfiguration : ConfigurationModuleBuilder
    {
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")] 
        public static readonly RequiredParameter<int> NetworkServicePort = new RequiredParameter<int>();

        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")] 
        public static readonly RequiredImpl<ICodecFactory> NetworkServiceCodecFactory = new RequiredImpl<ICodecFactory>();

        public static ConfigurationModule ConfigurationModule
        {
            get
            {
                return new NetworkServiceConfiguration()
                    .BindNamedParameter(GenericType<NetworkServiceOptions.NetworkServicePort>.Class, NetworkServicePort)
                    .BindNamedParameter(GenericType<NamingConfigurationOptions.NameServerPort>.Class,
                                        NamingConfiguration.NameServerPort)
                    .BindNamedParameter(GenericType<NamingConfigurationOptions.NameServerAddress>.Class,
                                        NamingConfiguration.NameServerAddress)
                    .BindImplementation(GenericType<ICodecFactory>.Class, NetworkServiceCodecFactory)
                    .Build();
            }
        }
    }
}
