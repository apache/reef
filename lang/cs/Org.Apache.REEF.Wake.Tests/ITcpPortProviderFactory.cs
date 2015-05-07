/*
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

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Wake.Tests
{
    /// <summary>
    /// Creates new instances of ITcpPortProvider .
    /// </summary>
    [DefaultImplementation(typeof(DefaultTcpPortProviderFactory))]
    public interface ITcpPortProviderFactory
    {
        /// <summary>
        /// Constructs a DefaultTcpPortProviderFactory
        /// available port.
        /// </summary>
        /// <param name="tcpPortRangeStart">Start port number for range provider</param>
        /// <param name="tcpPortRangeCount">Number of ports available on the range</param>
        ITcpPortProvider GetInstance(int tcpPortRangeStart, int tcpPortRangeCount);

        /// <summary>
        /// Constructs a DefaultTcpPortProviderFactory.
        /// </summary>
        /// <param name="tcpPortRangeStart">Start port number for range provider</param>
        /// <param name="tcpPortRangeCount">Number of ports available on the range</param>
        /// <param name="tcpPortRangeTryCount">Max number of ports to be delivered</param>
        ITcpPortProvider GetInstance(int tcpPortRangeStart, int tcpPortRangeCount, int tcpPortRangeTryCount);

        /// <summary>
        /// Constructs a TcpPortProvider
        /// </summary>
        ITcpPortProvider GetInstance();
    }
}