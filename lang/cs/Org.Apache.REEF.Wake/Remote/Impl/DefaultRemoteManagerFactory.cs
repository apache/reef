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

using System.Net;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Wake.Impl
{
    /// <summary>
    /// An implementation of IRemoteManagerFactory for DefaultRemoteManager.
    /// </summary>
    internal sealed class DefaultRemoteManagerFactory : IRemoteManagerFactory
    {
        private readonly ITcpPortProvider _tcpPortProvider;
        private readonly ITcpClientConnectionFactory _tcpClientFactory;
        private readonly ILocalAddressProvider _localAddressProvider;

        [Inject]
        private DefaultRemoteManagerFactory(
            ITcpPortProvider tcpPortProvider,  
            ITcpClientConnectionFactory tcpClientFactory, 
            ILocalAddressProvider localAddressProvider)
        {
            _tcpPortProvider = tcpPortProvider;
            _localAddressProvider = localAddressProvider;
            _tcpClientFactory = tcpClientFactory;
        }

        /// <summary>
        /// Gives DefaultRemoteManager instance
        /// </summary>
        /// <typeparam name="T">Type of Message</typeparam>
        /// <param name="localAddress">Local IP address</param>
        /// <param name="port">Port number</param>
        /// <param name="codec">Codec for message type T</param>
        /// <returns>IRemoteManager instance</returns>
        public IRemoteManager<T> GetInstance<T>(IPAddress localAddress, int port, ICodec<T> codec)
        {
            return new DefaultRemoteManager<T>(localAddress, port, codec, _tcpPortProvider, _localAddressProvider, _tcpClientFactory);
        }

        /// <summary>
        /// Gives DefaultRemoteManager instance
        /// </summary>
        /// <typeparam name="T">Message type</typeparam>
        /// <param name="localAddress">Local IP address</param>
        /// <param name="codec">Codec for message type</param>
        /// <returns>IRemoteManager instance</returns>
        public IRemoteManager<T> GetInstance<T>(IPAddress localAddress, ICodec<T> codec)
        {
            return new DefaultRemoteManager<T>(localAddress, 0, codec, _tcpPortProvider, _localAddressProvider, _tcpClientFactory);
        }

        /// <summary>
        /// Gives DefaultRemoteManager instance
        /// </summary>
        /// <typeparam name="T">Message type</typeparam>
        /// <param name="codec">Codec for message type</param>
        /// <returns>IRemoteManager instance</returns>
        public IRemoteManager<T> GetInstance<T>(ICodec<T> codec)
        {
            return new DefaultRemoteManager<T>(_localAddressProvider, codec, _tcpClientFactory);
        }
    }
}