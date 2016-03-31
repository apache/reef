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
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Network
{
    /// <summary>
    /// Configuration Module for the TCP Client provider
    /// </summary>
    public class TcpClientConfigurationModule : ConfigurationModuleBuilder
    {
        /// <summary>
        /// Number of times to retry the connection
        /// </summary>
        public static readonly OptionalParameter<int> MaxConnectionRetry = new OptionalParameter<int>();

        /// <summary>
        /// SLeep time between tries in milliseconds.
        /// </summary>
        public static readonly OptionalParameter<int> SleepTime = new OptionalParameter<int>();

        public static readonly ConfigurationModule ConfigurationModule = new TcpClientConfigurationModule()
            .BindSetEntry<DriverConfigurationProviders, TcpClientConfigurationProvider, IConfigurationProvider>(
                GenericType<DriverConfigurationProviders>.Class, GenericType<TcpClientConfigurationProvider>.Class)
            .BindNamedParameter(GenericType<ConnectionRetryCount>.Class, MaxConnectionRetry)
            .BindNamedParameter(GenericType<SleepTimeInMs>.Class, SleepTime)
            .Build();
    }
}
