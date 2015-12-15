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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Manages incoming and outgoing messages between remote hosts.
    /// </summary>
    /// <typeparam name="T">Message type T.</typeparam>
    internal sealed class StreamingRemoteManager<T> : IRemoteManager<T>
    {
        private readonly ObserverContainer<T> _observerContainer;
        private readonly StreamingTransportServer<IRemoteEvent<T>> _server;
        private readonly ConcurrentDictionary<IPEndPoint, ProxyObserver> _cachedClients;
        private readonly IStreamingCodec<IRemoteEvent<T>> _remoteEventCodec;

        /// <summary>
        /// Constructs a DefaultRemoteManager listening on the specified address and
        /// a specific port.
        /// </summary>
        /// <param name="localAddress">The address to listen on</param>
        /// <param name="tcpPortProvider">Tcp port provider</param>
        /// <param name="streamingCodec">Streaming codec</param>
        internal StreamingRemoteManager(IPAddress localAddress, ITcpPortProvider tcpPortProvider, IStreamingCodec<T> streamingCodec)
        {
            if (localAddress == null)
            {
                throw new ArgumentNullException("localAddress");
            }

            _observerContainer = new ObserverContainer<T>();
            _cachedClients = new ConcurrentDictionary<IPEndPoint, ProxyObserver>();
            _remoteEventCodec = new RemoteEventStreamingCodec<T>(streamingCodec);

            // Begin to listen for incoming messages
            _server = new StreamingTransportServer<IRemoteEvent<T>>(localAddress, _observerContainer, tcpPortProvider, _remoteEventCodec);
            _server.Run();

            LocalEndpoint = _server.LocalEndpoint;  
            Identifier = new SocketRemoteIdentifier(LocalEndpoint);
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
                StreamingTransportClient<IRemoteEvent<T>> client =
                    new StreamingTransportClient<IRemoteEvent<T>>(remoteEndpoint, _observerContainer, _remoteEventCodec);

                remoteObserver = new ProxyObserver(client,
                    () =>
                    {
                        ProxyObserver val;
                        _cachedClients.TryRemove(remoteEndpoint, out val);
                    });
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

            if (observer == null)
            {
                throw new ArgumentNullException("observer");
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
            private readonly StreamingTransportClient<IRemoteEvent<T>> _client;

            private readonly Action _removeFromCache;

            /// <summary>
            /// Create new ProxyObserver
            /// </summary>
            /// <param name="client">The connected WritableTransport client used to send
            /// messages to remote host</param>
            /// <param name="removeFromCache">Action that is used to remove proxy observer from cache</param>
            public ProxyObserver(StreamingTransportClient<IRemoteEvent<T>> client, Action removeFromCache)
            {
                _client = client;
                _removeFromCache = removeFromCache;
            }

            /// <summary>
            /// Send the message to the remote host
            /// </summary>
            /// <param name="message">The message to send</param>
            public void OnNext(T message)
            {
                IRemoteEvent<T> remoteEvent = new RemoteEvent<T>(_client.Link.LocalEndpoint,
                    _client.Link.RemoteEndpoint,
                    message);

                _client.Send(remoteEvent);
            }

            /// <summary>
            /// Close underlying WritableTransport client
            /// </summary>
            public void Dispose()
            {
                _client.Dispose();
            }

            public void OnError(Exception error)
            {
                throw error;
            }

            public void OnCompleted()
            {
                Dispose();
                _removeFromCache();
            }
        }
    }
}
