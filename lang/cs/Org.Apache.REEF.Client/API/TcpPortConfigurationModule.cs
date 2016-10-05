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
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Client.API
{
    /// <summary>
    /// Configuration Module for the TCP port provider.
    /// </summary>
    [Unstable("0.13", "Move to another namespace.")]
    public sealed class TcpPortConfigurationModule : ConfigurationModuleBuilder
    {
        /// <summary>
        /// Port number range start for listening on tcp ports.
        /// </summary>
        public static readonly RequiredParameter<int> PortRangeStart = new RequiredParameter<int>();

        /// <summary>
        /// Seed for the random port number generator.
        /// </summary>
        public static readonly OptionalParameter<int> PortRangeSeed = new OptionalParameter<int>();

        /// <summary>
        /// Port number count in the range for listening on tcp ports.
        /// </summary>
        public static readonly RequiredParameter<int> PortRangeCount = new RequiredParameter<int>();

        /// <summary>
        /// Count of tries to get a tcp port in the port range.
        /// </summary>
        public static readonly OptionalParameter<int> PortRangeTryCount = new OptionalParameter<int>();

        public static readonly ConfigurationModule ConfigurationModule = new TcpPortConfigurationModule()
            .BindSetEntry<DriverConfigurationProviders, TcpPortConfigurationProvider, IConfigurationProvider>(
                GenericType<DriverConfigurationProviders>.Class, GenericType<TcpPortConfigurationProvider>.Class)
            .BindNamedParameter(GenericType<TcpPortRangeStart>.Class, PortRangeStart)
            .BindNamedParameter(GenericType<TcpPortRangeSeed>.Class, PortRangeSeed)
            .BindNamedParameter(GenericType<TcpPortRangeCount>.Class, PortRangeCount)
            .BindNamedParameter(GenericType<TcpPortRangeTryCount>.Class, PortRangeTryCount)
            .Build();
    }
}