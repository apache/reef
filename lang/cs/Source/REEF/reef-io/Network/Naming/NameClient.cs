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

using Org.Apache.Reef.Common.io;
using Org.Apache.Reef.IO.Network.Naming.Codec;
using Org.Apache.Reef.IO.Network.Naming.Events;
using Org.Apache.Reef.Utilities.Diagnostics;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Wake;
using Org.Apache.Reef.Wake.Remote;
using Org.Apache.Reef.Wake.Remote.Impl;
using Org.Apache.Reef.Wake.RX;
using Org.Apache.Reef.Wake.RX.Impl;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reactive;

namespace Org.Apache.Reef.IO.Network.Naming
{
    /// <summary>
    /// Client for the Reef name service. 
    /// Used to register, unregister, and lookup IP Addresses of known hosts.
    /// </summary>
    public class NameClient : INameClient
    {
        private static Logger _logger = Logger.GetLogger(typeof(NameClient));

        private BlockingCollection<NamingLookupResponse> _lookupResponseQueue;
        private BlockingCollection<NamingGetAllResponse> _getAllResponseQueue;
        private BlockingCollection<NamingRegisterResponse> _registerResponseQueue;
        private BlockingCollection<NamingUnregisterResponse> _unregisterResponseQueue;

        private TransportClient<NamingEvent> _client;

        private NameLookupClient _lookupClient;
        private NameRegisterClient _registerClient;

        private bool _disposed;

        /// <summary>
        /// Constructs a NameClient to register, lookup, and unregister IPEndpoints
        /// with the NameServer.
        /// </summary>
        /// <param name="remoteAddress">The ip address of the NameServer</param>
        /// <param name="remotePort">The port of the NameServer</param>
        [Inject]
        public NameClient(
            [Parameter(typeof(NamingConfigurationOptions.NameServerAddress))] string remoteAddress, 
            [Parameter(typeof(NamingConfigurationOptions.NameServerPort))] int remotePort)
        {
            IPEndPoint remoteEndpoint = new IPEndPoint(IPAddress.Parse(remoteAddress), remotePort);
            Initialize(remoteEndpoint);
            _disposed = false;
        }

        /// <summary>
        /// Constructs a NameClient to register, lookup, and unregister IPEndpoints
        /// with the NameServer.
        /// </summary>
        /// <param name="remoteEndpoint">The endpoint of the NameServer</param>
        public NameClient(IPEndPoint remoteEndpoint) 
        {
            Initialize(remoteEndpoint);
            _disposed = false;
        }

        /// <summary>
        /// Synchronously registers the identifier with the NameService.  
        /// Overwrites the previous mapping if the identifier has already 
        /// been registered.
        /// </summary>
        /// <param name="id">The key used to map the remote endpoint</param>
        /// <param name="endpoint">The endpoint to map</param>
        public void Register(string id, IPEndPoint endpoint)
        {
            if (id == null)
            {
                Exceptions.Throw(new ArgumentNullException("id"), _logger);
            }
            if (endpoint == null)
            {
                Exceptions.Throw(new ArgumentNullException("endpoint"), _logger);
            }

            _logger.Log(Level.Info, "Registering id: " + id + ", and endpoint: " + endpoint);
            _registerClient.Register(id, endpoint);
        }

        /// <summary>
        /// Synchronously unregisters the remote identifier with the NameService
        /// </summary>
        /// <param name="id">The identifier to unregister</param>
        public void Unregister(string id)
        {
            if (id == null)
            {
                Exceptions.Throw(new ArgumentNullException("id"), _logger);
            }

            _logger.Log(Level.Info, "Unregistering id: " + id);
            _registerClient.Unregister(id);
        }

        /// <summary>
        /// Synchronously looks up the IPEndpoint for the registered identifier.
        /// </summary>
        /// <param name="id">The identifier to look up</param>
        /// <returns>The mapped IPEndpoint for the identifier, or null if
        /// the identifier has not been registered with the NameService</returns>
        public IPEndPoint Lookup(string id)
        {
            if (id == null)
            {
                Exceptions.Throw(new ArgumentNullException("id"), _logger);
            }

            List<NameAssignment> assignments = Lookup(new List<string> { id });
            if (assignments != null && assignments.Count > 0)
            {
                return assignments.First().Endpoint;
            }

            return null;
        }

        /// <summary>
        /// Synchronously looks up the IPEndpoint for each of the registered identifiers in the list.
        /// </summary>
        /// <param name="ids">The list of identifiers to look up</param>
        /// <returns>The list of NameAssignments representing a pair of identifer
        /// and mapped IPEndpoint for that identifier.  If any of the requested identifiers
        /// are not registered with the NameService, their corresponding NameAssignment
        /// IPEndpoint value will be null.</returns>
        public List<NameAssignment> Lookup(List<string> ids)
        {
            if (ids == null || ids.Count == 0)
            {
                Exceptions.Throw(new ArgumentNullException("ids cannot be null or empty"), _logger);
            }

            _logger.Log(Level.Verbose, "Looking up ids");
            List<NameAssignment> assignments = _lookupClient.Lookup(ids);
            if (assignments != null)
            {
                return assignments;
            }
            Exceptions.Throw(new WakeRuntimeException("NameClient failed to look up ids."), _logger);
            return null;  //above line will throw exception. So null will never be returned.
        }

        /// <summary>
        /// Restart the name client in case of failure.
        /// </summary>
        /// <param name="serverEndpoint">The new server endpoint to connect to</param>
        public void Restart(IPEndPoint serverEndpoint)
        {
            _client.Dispose();
            Initialize(serverEndpoint);
        }

        /// <summary>
        /// Releases resources used by NameClient
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }
            if (disposing)
            {
                _client.Dispose();
            }
            _disposed = true;
        }

        /// <summary>
        /// Create a new transport client connected to the NameServer at the given remote endpoint.
        /// </summary>
        /// <param name="serverEndpoint">The NameServer endpoint to connect to.</param>
        private void Initialize(IPEndPoint serverEndpoint)
        {
            _lookupResponseQueue = new BlockingCollection<NamingLookupResponse>();
            _getAllResponseQueue = new BlockingCollection<NamingGetAllResponse>();
            _registerResponseQueue = new BlockingCollection<NamingRegisterResponse>();
            _unregisterResponseQueue = new BlockingCollection<NamingUnregisterResponse>();

            IObserver<TransportEvent<NamingEvent>> clientHandler = CreateClientHandler();
            ICodec<NamingEvent> codec = CreateClientCodec();
            _client = new TransportClient<NamingEvent>(serverEndpoint, codec, clientHandler);

            _lookupClient = new NameLookupClient(_client, _lookupResponseQueue, _getAllResponseQueue);
            _registerClient = new NameRegisterClient(_client, _registerResponseQueue, _unregisterResponseQueue);
        }

        /// <summary>
        /// Create handler to handle async responses from the NameServer.
        /// </summary>
        /// <returns>The client handler to manage responses from the NameServer</returns>
        private IObserver<TransportEvent<NamingEvent>> CreateClientHandler()
        {
            PubSubSubject<NamingEvent> subject = new PubSubSubject<NamingEvent>();
            subject.Subscribe(Observer.Create<NamingLookupResponse>(msg => HandleResponse(_lookupResponseQueue, msg)));
            subject.Subscribe(Observer.Create<NamingGetAllResponse>(msg => HandleResponse(_getAllResponseQueue, msg)));
            subject.Subscribe(Observer.Create<NamingRegisterResponse>(msg => HandleResponse(_registerResponseQueue, msg)));
            subject.Subscribe(Observer.Create<NamingUnregisterResponse>(msg => HandleResponse(_unregisterResponseQueue, msg)));
            return new ClientObserver(subject);
        }

        /// <summary>
        /// Create the codec used to serialize/deserialize NamingEvent messages
        /// </summary>
        /// <returns>The serialization codec</returns>
        private ICodec<NamingEvent> CreateClientCodec()
        {
            MultiCodec<NamingEvent> codec = new MultiCodec<NamingEvent>();
            codec.Register(new NamingLookupRequestCodec(), "org.apache.reef.io.network.naming.serialization.NamingLookupRequest");
            codec.Register(new NamingLookupResponseCodec(), "org.apache.reef.io.network.naming.serialization.NamingLookupResponse");
            NamingRegisterRequestCodec requestCodec = new NamingRegisterRequestCodec();
            codec.Register(requestCodec, "org.apache.reef.io.network.naming.serialization.NamingRegisterRequest");
            codec.Register(new NamingRegisterResponseCodec(requestCodec), "org.apache.reef.io.network.naming.serialization.NamingRegisterResponse");
            codec.Register(new NamingUnregisterRequestCodec(), "org.apache.reef.io.network.naming.serialization.NamingUnregisterRequest");
            return codec;
        }

        private void HandleResponse<T>(BlockingCollection<T> queue, T message)
        {
            queue.Add(message);
        }

        /// <summary>
        /// Helper class used to handle response events from the NameServer.
        /// Delegates the event to the appropriate response queue depending on
        /// its event type.
        /// </summary>
        private class ClientObserver : AbstractObserver<TransportEvent<NamingEvent>>
        {
            private IObserver<NamingEvent> _handler;

            public ClientObserver(IObserver<NamingEvent> handler)
            {
                _handler = handler;
            }

            public override void OnNext(TransportEvent<NamingEvent> value)
            {
                NamingEvent message = value.Data;
                message.Link = value.Link;
                _handler.OnNext(message);
            }
        }
    }
}
