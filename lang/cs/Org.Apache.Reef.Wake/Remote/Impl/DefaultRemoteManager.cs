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

using Org.Apache.Reef.Utilities.Diagnostics;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Wake.Util;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reactive;
using System.Threading.Tasks;

namespace Org.Apache.Reef.Wake.Remote.Impl
{
    /// <summary>
    /// Manages incoming and outgoing messages between remote hosts.
    /// </summary>
    public class DefaultRemoteManager<T> : IRemoteManager<T>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultRemoteManager<T>));

        private ObserverContainer<T> _observerContainer;
        private TransportServer<IRemoteEvent<T>> _server; 
        private Dictionary<IPEndPoint, ProxyObserver> _cachedClients;
        private ICodec<IRemoteEvent<T>> _codec;

        /// <summary>
        /// Constructs a DefaultRemoteManager listening on the specified address and any
        /// available port.
        /// </summary>
        /// <param name="localAddress">The address to listen on</param>
        /// <param name="codec">The codec used for serializing messages</param>
        public DefaultRemoteManager(IPAddress localAddress, ICodec<T> codec) : this(localAddress, 0, codec)
        {
        }

        /// <summary>
        /// Constructs a DefaultRemoteManager listening on the specified IPEndPoint.
        /// </summary>
        /// <param name="localEndpoint">The endpoint to listen on</param>
        /// <param name="codec">The codec used for serializing messages</param>
        public DefaultRemoteManager(IPEndPoint localEndpoint, ICodec<T> codec)
        {
            if (localEndpoint == null)
            {
                throw new ArgumentNullException("localEndpoint");
            }
            if (localEndpoint.Port < 0)
            {
                throw new ArgumentException("Listening port must be greater than or equal to zero");
            }
            if (codec == null)
            {
                throw new ArgumentNullException("codec");
            }

            _codec = new RemoteEventCodec<T>(codec);
            _observerContainer = new ObserverContainer<T>();
            _cachedClients = new Dictionary<IPEndPoint, ProxyObserver>();

            // Begin to listen for incoming messages
            _server = new TransportServer<IRemoteEvent<T>>(localEndpoint, _observerContainer, _codec);
            _server.Run();

            LocalEndpoint = _server.LocalEndpoint;
            Identifier = new SocketRemoteIdentifier(LocalEndpoint);
        }

        /// <summary>
        /// Constructs a DefaultRemoteManager listening on the specified address and any
        /// available port.
        /// </summary>
        /// <param name="localAddress">The address to listen on</param>
        /// <param name="port">The port to listen on</param>
        /// <param name="codec">The codec used for serializing messages</param>
        public DefaultRemoteManager(IPAddress localAddress, int port, ICodec<T> codec)
        {
            if (localAddress == null)
            {
                throw new ArgumentNullException("localAddress");
            }
            if (port < 0)
            {
                throw new ArgumentException("Listening port must be greater than or equal to zero");
            }
            if (codec == null)
            {
                throw new ArgumentNullException("codec");
            }

            _observerContainer = new ObserverContainer<T>();
            _codec = new RemoteEventCodec<T>(codec);
            _cachedClients = new Dictionary<IPEndPoint, ProxyObserver>();

            IPEndPoint localEndpoint = new IPEndPoint(localAddress, port);

            // Begin to listen for incoming messages
            _server = new TransportServer<IRemoteEvent<T>>(localEndpoint, _observerContainer, _codec);
            _server.Run();

            LocalEndpoint = _server.LocalEndpoint;
            Identifier = new SocketRemoteIdentifier(LocalEndpoint);
        }

        /// <summary>
        /// Constructs a DefaultRemoteManager. Does not listen for incoming messages.
        /// </summary>
        /// <param name="codec">The codec used for serializing messages</param>
        public DefaultRemoteManager(ICodec<T> codec)
        {
            using (LOGGER.LogFunction("DefaultRemoteManager::DefaultRemoteManager"))
            {
                if (codec == null)
                {
                    throw new ArgumentNullException("codec");
                }

                _observerContainer = new ObserverContainer<T>();
                _codec = new RemoteEventCodec<T>(codec);
                _cachedClients = new Dictionary<IPEndPoint, ProxyObserver>();

                LocalEndpoint = new IPEndPoint(NetworkUtils.LocalIPAddress, 0);
                Identifier = new SocketRemoteIdentifier(LocalEndpoint);
            }
        }

        /// <summary>
        /// Gets the RemoteIdentifier for the DefaultRemoteManager
        /// </summary>
        public IRemoteIdentifier Identifier { get; private set; }

        /// <summary>
        /// Gets the local IPEndPoint for the DefaultRemoteManager
        /// </summary>
        public IPEndPoint LocalEndpoint { get; private set; }

        /// <summary>
        /// Returns an IObserver used to send messages to the remote host at
        /// the specified IPEndpoint.
        /// </summary>
        /// <param name="remoteEndpoint">The IPEndpoint of the remote host</param>
        /// <returns>An IObserver used to send messages to the remote host</returns>
        public IObserver<T> GetRemoteObserver(RemoteEventEndPoint<T> remoteEndpoint)
        {
            if (remoteEndpoint == null)
            {
                throw new ArgumentNullException("remoteEndpoint");
            }

            SocketRemoteIdentifier id = remoteEndpoint.Id as SocketRemoteIdentifier;
            if (id == null)
            {
                throw new ArgumentException("ID not supported");
            }

            return GetRemoteObserver(id.Addr);
        }

        /// <summary>
        /// Returns an IObserver used to send messages to the remote host at
        /// the specified IPEndpoint.
        /// </summary>
        /// <param name="remoteEndpoint">The IPEndpoint of the remote host</param>
        /// <returns>An IObserver used to send messages to the remote host</returns>
        public IObserver<T> GetRemoteObserver(IPEndPoint remoteEndpoint)
        {
            if (remoteEndpoint == null)
            {
                throw new ArgumentNullException("remoteEndpoint");
            }

            ProxyObserver remoteObserver;
            if (!_cachedClients.TryGetValue(remoteEndpoint, out remoteObserver))
            {
                TransportClient<IRemoteEvent<T>> client = 
                    new TransportClient<IRemoteEvent<T>>(remoteEndpoint, _codec, _observerContainer);

                remoteObserver = new ProxyObserver(client);
                _cachedClients[remoteEndpoint] = remoteObserver;
            }

            return remoteObserver;
        }

        /// <summary>
        /// Registers an IObserver used to handle incoming messages from the remote host
        /// at the specified IPEndPoint.
        /// The IDisposable that is returned can be used to unregister the IObserver.
        /// </summary>
        /// <param name="remoteEndpoint">The IPEndPoint of the remote host</param>
        /// <param name="observer">The IObserver to handle incoming messages</param>
        /// <returns>An IDisposable used to unregister the observer with</returns>
        public IDisposable RegisterObserver(RemoteEventEndPoint<T> remoteEndpoint, IObserver<T> observer)
        {
            if (remoteEndpoint == null)
            {
                throw new ArgumentNullException("remoteEndpoint");
            }

            SocketRemoteIdentifier id = remoteEndpoint.Id as SocketRemoteIdentifier;
            if (id == null)
            {
                throw new ArgumentException("ID not supported");
            }

            return RegisterObserver(id.Addr, observer);
        }

        /// <summary>
        /// Registers an IObserver used to handle incoming messages from the remote host
        /// at the specified IPEndPoint.
        /// The IDisposable that is returned can be used to unregister the IObserver.
        /// </summary>
        /// <param name="remoteEndpoint">The IPEndPoint of the remote host</param>
        /// <param name="observer">The IObserver to handle incoming messages</param>
        /// <returns>An IDisposable used to unregister the observer with</returns>
        public IDisposable RegisterObserver(IPEndPoint remoteEndpoint, IObserver<T> observer)
        {
            if (remoteEndpoint == null)
            {
                throw new ArgumentNullException("remoteEndpoint");
            }
            if (observer == null)
            {
                throw new ArgumentNullException("observer");
            }

            return _observerContainer.RegisterObserver(remoteEndpoint, observer);
        }

        /// <summary>
        /// Registers an IObserver used to handle incoming messages from the remote host
        /// at the specified IPEndPoint.
        /// The IDisposable that is returned can be used to unregister the IObserver.
        /// </summary>
        /// <param name="observer">The IObserver to handle incoming messages</param>
        /// <returns>An IDisposable used to unregister the observer with</returns>
        public IDisposable RegisterObserver(IObserver<IRemoteMessage<T>> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException("observer");
            }

            return _observerContainer.RegisterObserver(observer);
        }

        /// <summary>
        /// Release all resources for the DefaultRemoteManager.
        /// </summary>
        public void Dispose()
        {
            foreach (ProxyObserver cachedClient in _cachedClients.Values)
            {
                cachedClient.Dispose();
            }

            if (_server != null)
            {
                _server.Dispose();
            }
        }

        /// <summary>
        /// Observer to send messages to connected remote host
        /// </summary>
        private class ProxyObserver : IObserver<T>, IDisposable
        {
            private TransportClient<IRemoteEvent<T>> _client;
            private int _messageCount;

            /// <summary>
            /// Create new ProxyObserver
            /// </summary>
            /// <param name="client">The connected transport client used to send
            /// messages to remote host</param>
            public ProxyObserver(TransportClient<IRemoteEvent<T>> client)
            {
                _client = client;
                _messageCount = 0;
            }

            /// <summary>
            /// Send the message to the remote host
            /// </summary>
            /// <param name="message">The message to send</param>
            public void OnNext(T message)
            {
                IRemoteEvent<T> remoteEvent = new RemoteEvent<T>(_client.Link.LocalEndpoint, _client.Link.RemoteEndpoint, message)
                {
                    Sink = "default",
                    Sequence = _messageCount
                };

                _messageCount++;
                _client.Send(remoteEvent);
            }

            /// <summary>
            /// Close underlying transport client
            /// </summary>
            public void Dispose()
            {
                _client.Dispose();
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }
        }
    }
}
