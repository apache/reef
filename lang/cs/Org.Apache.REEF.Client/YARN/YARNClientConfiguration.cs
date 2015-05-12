/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Local.Parameters;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Client.YARN
{
    /// <summary>
    /// The Configuration for the YARN Client
    /// </summary>
    public sealed class YARNClientConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        /// Configuration provides whose Configuration will be merged into all Driver Configuration.
        /// </summary>
        public static readonly OptionalImpl<IConfigurationProvider> DriverConfigurationProvider = new OptionalImpl<IConfigurationProvider>();

        /// <summary>
        /// Start of the tcp port range for listening.
        /// </summary>
        public static readonly OptionalParameter<int> TcpPortRangeStartParameter = new OptionalParameter<int>();

        /// <summary>
        /// Number of port for the tcp port range for listening.
        /// </summary>
        public static readonly OptionalParameter<int> TcpPortRangeCountParameter = new OptionalParameter<int>();

        /// <summary>
        /// Max number of times we will deliver a port from the tcp port range.
        /// </summary>
        public static readonly OptionalParameter<int> TcpPortRangeTryCountParameter = new OptionalParameter<int>();

        /// <summary>
        /// Seed for the number number for determining which particular port to deliver from the range
        /// </summary>
        public static readonly OptionalParameter<int> TcpPortRangeSeedParameter = new OptionalParameter<int>();


        public static ConfigurationModule ConfigurationModule = new YARNClientConfiguration()
            .BindImplementation(GenericType<IREEFClient>.Class, GenericType<YARNClient>.Class)
            .BindSetEntry(GenericType<DriverConfigurationProviders>.Class, DriverConfigurationProvider)
            .BindNamedParameter(GenericType<TcpPortRangeStart>.Class, TcpPortRangeStartParameter)
            .BindNamedParameter(GenericType<TcpPortRangeCount>.Class, TcpPortRangeCountParameter)
            .BindNamedParameter(GenericType<TcpPortRangeTryCount>.Class, TcpPortRangeTryCountParameter)
            .BindNamedParameter(GenericType<TcpPortRangeSeed>.Class, TcpPortRangeSeedParameter)
            .Build();
    }
}