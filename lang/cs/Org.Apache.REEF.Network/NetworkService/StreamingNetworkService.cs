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
using System.Collections.Generic;
using System.Net;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Network.NetworkService.Codec;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.StreamingCodec;
using Org.Apache.REEF.Wake.Util;

namespace Org.Apache.REEF.Network.NetworkService
{
    /// <summary>
    /// Writable Network service used for Reef Task communication.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public class StreamingNetworkService<T> : INetworkService<T>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(StreamingNetworkService<>));

        private readonly IRemoteManager<NsMessage<T>> _remoteManager;
        private IIdentifier _localIdentifier;
        private readonly IDisposable _messageHandlerDisposable;
        private readonly Dictionary<IIdentifier, IConnection<T>> _connectionMap;
        private readonly INameClient _nameClient;

        /// <summary>
        /// Create a new Writable NetworkService.
        /// </summary>
        /// <param name="messageHandler">The observer to handle incoming messages</param>
        /// <param name="idFactory">The factory used to create IIdentifiers</param>
        /// <param name="nameClient">The name client used to register Ids</param>
        /// <param name="remoteManagerFactory">Writable RemoteManagerFactory to create a 
        /// Writable RemoteManager</param>
        /// <param name="codec">Codec for Network Service message</param>
        /// <param name="injector">Fork of the injector that created the Network service</param>
        [Inject]
        private StreamingNetworkService(
            IObserver<NsMessage<T>> messageHandler,
            IIdentifierFactory idFactory,
            INameClient nameClient,
            StreamingRemoteManagerFactory remoteManagerFactory,
            NsMessageStreamingCodec<T> codec,
            IInjector injector)
        {
            IPAddress localAddress = NetworkUtils.LocalIPAddress;
            _remoteManager = remoteManagerFactory.GetInstance(localAddress, codec);

            // Create and register incoming message handler
            // TODO[REEF-419] This should use the TcpPortProvider mechanism
            var anyEndpoint = new IPEndPoint(IPAddress.Any, 0);
            _messageHandlerDisposable = _remoteManager.RegisterObserver(anyEndpoint, messageHandler);

            _nameClient = nameClient;
            _connectionMap = new Dictionary<IIdentifier, IConnection<T>>();

            Logger.Log(Level.Info, "Started network service");
        }

        /// <summary>
        /// Name client for registering ids
        /// </summary>
        public INameClient NamingClient
        {
            get { return _nameClient; }
        }

        /// <summary>
        /// Open a new connection to the remote host registered to
        /// the name service with the given identifier
        /// </summary>
        /// <param name="destinationId">The identifier of the remote host</param>
        /// <returns>The IConnection used for communication</returns>
        public IConnection<T> NewConnection(IIdentifier destinationId)
        {
            if (_localIdentifier == null)
            {
                throw new IllegalStateException("Cannot open connection without first registering an ID");
            }

            IConnection<T> connection;
            if (_connectionMap.TryGetValue(destinationId, out connection))
            {
                return connection;
            }
            else
            {
                connection = new NsConnection<T>(_localIdentifier, destinationId,
                    NamingClient, _remoteManager, _connectionMap);

                _connectionMap[destinationId] = connection;
                return connection;
            }
        }

        /// <summary>
        /// Register the identifier for the NetworkService with the NameService.
        /// </summary>
        /// <param name="id">The identifier to register</param>
        public void Register(IIdentifier id)
        {
            Logger.Log(Level.Info, "Registering id {0} with network service.", id);

            _localIdentifier = id;
            NamingClient.Register(id.ToString(), _remoteManager.LocalEndpoint);

            Logger.Log(Level.Info, "End of Registering id {0} with network service.", id);
        }

        /// <summary>
        /// Unregister the identifier for the NetworkService with the NameService.
        /// </summary>
        public void Unregister()
        {
            if (_localIdentifier == null)
            {
                throw new IllegalStateException("Cannot unregister a non existant identifier");
            }

            NamingClient.Unregister(_localIdentifier.ToString());
            _localIdentifier = null;
            _messageHandlerDisposable.Dispose();
        }

        /// <summary>
        /// Dispose of the NetworkService's resources
        /// </summary>
        public void Dispose()
        {
            NamingClient.Dispose();
            _remoteManager.Dispose();

            Logger.Log(Level.Info, "Disposed of network service");
        }
    }
}
