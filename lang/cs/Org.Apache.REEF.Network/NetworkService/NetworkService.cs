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
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.NetworkService
{
    /// <summary>
    /// Network service used for Reef Task communication.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public class NetworkService<T> : INetworkService<T>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(NetworkService<>));

        private readonly IRemoteManager<NsMessage<T>> _remoteManager;
        private readonly IObserver<NsMessage<T>> _messageHandler; 
        private readonly ICodec<NsMessage<T>> _codec; 
        private IIdentifier _localIdentifier;
        private IDisposable _messageHandlerDisposable;
        private readonly Dictionary<IIdentifier, IConnection<T>> _connectionMap;

        /// <summary>
        /// Create a new NetworkService.
        /// </summary>
        /// <param name="nsPort">The port that the NetworkService will listen on</param>
        /// <param name="messageHandler">The observer to handle incoming messages</param>
        /// <param name="idFactory">The factory used to create IIdentifiers</param>
        /// <param name="codec">The codec used for serialization</param>
        /// <param name="nameClient"></param>
        /// <param name="localAddressProvider">The local address provider</param>
        /// <param name="remoteManagerFactory">Used to instantiate remote manager instances.</param>
        [Inject]
        public NetworkService(
            [Parameter(typeof(NetworkServiceOptions.NetworkServicePort))] int nsPort,
            IObserver<NsMessage<T>> messageHandler,
            IIdentifierFactory idFactory,
            ICodec<T> codec,
            INameClient nameClient,
            ILocalAddressProvider localAddressProvider,
            IRemoteManagerFactory remoteManagerFactory)
        {
            _codec = new NsMessageCodec<T>(codec, idFactory);

            IPAddress localAddress = localAddressProvider.LocalAddress;
            _remoteManager = remoteManagerFactory.GetInstance(localAddress, nsPort, _codec);
            _messageHandler = messageHandler;

            NamingClient = nameClient;
            _connectionMap = new Dictionary<IIdentifier, IConnection<T>>();

            LOGGER.Log(Level.Verbose, "Started network service");
        }

        /// <summary>
        /// Name client for registering ids
        /// </summary>
        public INameClient NamingClient { get; private set; }

        /// <summary>
        /// The remote manager of the network service.
        /// </summary>
        public IRemoteManager<NsMessage<T>> RemoteManager
        {
            get { return _remoteManager; }
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

            connection = new NsConnection<T>(_localIdentifier, destinationId, 
                NamingClient, _remoteManager, _connectionMap);

            _connectionMap[destinationId] = connection;
            return connection;
        }

        /// <summary>
        /// Register the identifier for the NetworkService with the NameService.
        /// </summary>
        /// <param name="id">The identifier to register</param>
        public void Register(IIdentifier id)
        {
            LOGGER.Log(Level.Verbose, "Registering id {0} with network service.", id);

            _localIdentifier = id;
            NamingClient.Register(id.ToString(), _remoteManager.LocalEndpoint);

            // Create and register incoming message handler
            var anyEndpoint = new IPEndPoint(IPAddress.Any, 0);
            _messageHandlerDisposable = _remoteManager.RegisterObserver(anyEndpoint, _messageHandler);

            LOGGER.Log(Level.Verbose, "End of Registering id {0} with network service.", id);
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

            LOGGER.Log(Level.Verbose, "Disposed of network service");
        }
    }
}
