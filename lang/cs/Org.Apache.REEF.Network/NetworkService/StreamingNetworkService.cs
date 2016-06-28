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
using Org.Apache.REEF.Wake.Remote.Impl;

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
        private readonly IDisposable _universalObserverDisposable;
        private readonly IDisposable _remoteMessageUniversalObserver;
        private readonly Dictionary<IIdentifier, IConnection<T>> _connectionMap;
        private readonly INameClient _nameClient;

        /// <summary>
        /// Create a new Writable NetworkService.
        /// </summary>
        /// <param name="universalObserver">The observer to handle incoming messages</param>
        /// <param name="nameClient">The name client used to register Ids</param>
        /// <param name="remoteManagerFactory">
        /// Writable RemoteManagerFactory to create a Writable RemoteManager
        /// </param>
        /// <param name="codec">Codec for Network Service message</param>
        /// <param name="localAddressProvider">The local address provider</param>
        [Inject]
        private StreamingNetworkService(
            IObserver<NsMessage<T>> universalObserver,
            INameClient nameClient,
            StreamingRemoteManagerFactory remoteManagerFactory,
            NsMessageStreamingCodec<T> codec,
            ILocalAddressProvider localAddressProvider)
            : this(universalObserver, null, nameClient, remoteManagerFactory, codec, localAddressProvider)
        {
        }

        /// <summary>
        /// Create a new Writable NetworkService
        /// </summary>
        /// <param name="remoteMessageUniversalObserver">The observer to handle incoming messages</param>
        /// <param name="nameClient">The name client used to register Ids</param>
        /// <param name="remoteManagerFactory">
        /// Writable RemoteManagerFactory to create a Writable RemoteManager
        /// </param>
        /// <param name="codec">Codec for Network Service message</param>
        /// <param name="localAddressProvider">The local address provider</param>
        [Inject]
        private StreamingNetworkService(
            IObserver<IRemoteMessage<NsMessage<T>>> remoteMessageUniversalObserver,
            INameClient nameClient,
            StreamingRemoteManagerFactory remoteManagerFactory,
            NsMessageStreamingCodec<T> codec,
            ILocalAddressProvider localAddressProvider)
            : this(null, remoteMessageUniversalObserver, nameClient, remoteManagerFactory, codec, localAddressProvider)
        {
        }

        [Inject]
        private StreamingNetworkService(
            IObserver<NsMessage<T>> universalObserver,
            IObserver<IRemoteMessage<NsMessage<T>>> remoteMessageUniversalObserver,
            INameClient nameClient,
            StreamingRemoteManagerFactory remoteManagerFactory,
            NsMessageStreamingCodec<T> codec,
            ILocalAddressProvider localAddressProvider)
        {
            _remoteManager = remoteManagerFactory.GetInstance(localAddressProvider.LocalAddress, codec);

            if (universalObserver != null)
            {
                // Create and register incoming message handler
                // TODO[REEF-419] This should use the TcpPortProvider mechanism
                var anyEndpoint = new IPEndPoint(IPAddress.Any, 0);
                _universalObserverDisposable = _remoteManager.RegisterObserver(anyEndpoint, universalObserver);
            }
            else
            {
                _universalObserverDisposable = null;
            }

            _remoteMessageUniversalObserver = remoteMessageUniversalObserver != null ?
                _remoteManager.RegisterObserver(remoteMessageUniversalObserver) : null;

            _nameClient = nameClient;
            _connectionMap = new Dictionary<IIdentifier, IConnection<T>>();

            Logger.Log(Level.Verbose, "Started network service");
        }

        /// <summary>
        /// Name client for registering ids
        /// </summary>
        public INameClient NamingClient
        {
            get { return _nameClient; }
        }

        /// <summary>
        /// RemoteManager for registering Observers.
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
            Logger.Log(Level.Verbose, "Registering id {0} with network service.", id);

            _localIdentifier = id;
            NamingClient.Register(id.ToString(), _remoteManager.LocalEndpoint);

            Logger.Log(Level.Verbose, "End of Registering id {0} with network service.", id);
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

            if (_universalObserverDisposable != null)
            {
                _universalObserverDisposable.Dispose();
            }

            if (_remoteMessageUniversalObserver != null)
            {
                _remoteMessageUniversalObserver.Dispose();
            }
        }

        /// <summary>
        /// Dispose of the NetworkService's resources
        /// </summary>
        public void Dispose()
        {
            NamingClient.Dispose();
            _remoteManager.Dispose();

            Logger.Log(Level.Verbose, "Disposed of network service");
        }
    }
}