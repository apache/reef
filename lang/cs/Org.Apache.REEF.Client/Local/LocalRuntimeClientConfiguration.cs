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
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Client.Local
{
    public sealed class LocalRuntimeClientConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        /// The number of threads or evaluators available to the resourcemanager.
        /// </summary>
        /// <remarks>
        /// This is the upper limit on the number of
        /// Evaluators that the local resourcemanager will hand out concurrently. This simulates the size of a physical cluster
        /// in terms of the number of slots available on it with one important caveat: The Driver is not counted against this
        /// number.
        /// </remarks>
        public static readonly OptionalParameter<int> NumberOfEvaluators = new OptionalParameter<int>();

        /// <summary>
        /// The folder in which the sub-folders, one per job, will be created.
        /// </summary>
        /// <remarks>
        /// If none is given, the temp directory is used.
        /// </remarks>
        public static readonly OptionalParameter<string> RuntimeFolder = new OptionalParameter<string>();

        /// <summary>
        /// Configuration provides whose Configuration will be merged into all Driver Configuration.
        /// </summary>
        public static readonly OptionalImpl<IConfigurationProvider> DriverConfigurationProvider =
            new OptionalImpl<IConfigurationProvider>();

        /// <summary>
        /// Start of the tcp port range for listening.
        /// </summary>
        public static readonly OptionalParameter<int> TcpPortRangeStartParameter = new OptionalParameter<int>();

        /// <summary>
        /// Number of ports for the tcp port range for listening.
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

        public static ConfigurationModule ConfigurationModule = new LocalRuntimeClientConfiguration()
            .BindImplementation(GenericType<IREEFClient>.Class, GenericType<LocalClient>.Class)
            .BindNamedParameter(GenericType<LocalRuntimeDirectory>.Class, RuntimeFolder)
            .BindNamedParameter(GenericType<NumberOfEvaluators>.Class, NumberOfEvaluators)
            .BindSetEntry(GenericType<DriverConfigurationProviders>.Class, DriverConfigurationProvider)
            .BindNamedParameter(GenericType<TcpPortRangeStart>.Class, TcpPortRangeStartParameter)
            .BindNamedParameter(GenericType<TcpPortRangeCount>.Class, TcpPortRangeCountParameter)
            .BindNamedParameter(GenericType<TcpPortRangeTryCount>.Class, TcpPortRangeTryCountParameter)
            .BindNamedParameter(GenericType<TcpPortRangeSeed>.Class, TcpPortRangeSeedParameter)
            .Build();
    }
}