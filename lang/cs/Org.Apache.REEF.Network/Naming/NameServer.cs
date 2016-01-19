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
using System.Linq;
using System.Net;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Network.Naming.Codec;
using Org.Apache.REEF.Network.Naming.Events;
using Org.Apache.REEF.Network.Naming.Observers;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Wake.RX;
using Org.Apache.REEF.Wake.RX.Impl;
using Org.Apache.REEF.Wake.Util;

namespace Org.Apache.REEF.Network.Naming
{
    /// <summary>
    /// Service that manages names and IPEndpoints for well known hosts.
    /// Can register, unregister, and look up IPAddresses using a string identifier.
    /// </summary>
    public class NameServer : INameServer
    {
        private static readonly Logger _logger = Logger.GetLogger(typeof(NameServer));

        private readonly TransportServer<NamingEvent> _server;
        private readonly Dictionary<string, IPEndPoint> _idToAddrMap;
        
        /// <summary>
        /// Create a new NameServer to run on the specified port.
        /// </summary>
        /// <param name="port">The port to listen for incoming connections on.</param>
        /// <param name="tcpPortProvider">If port is 0, this interface provides 
        /// a port range to try.
        /// </param>
        [Inject]
        private NameServer(
            [Parameter(typeof(NamingConfigurationOptions.NameServerPort))] int port,
            ITcpPortProvider tcpPortProvider)
        {
            IObserver<TransportEvent<NamingEvent>> handler = CreateServerHandler();
            _idToAddrMap = new Dictionary<string, IPEndPoint>();
            ICodec<NamingEvent> codec = CreateServerCodec();

            // Start transport server, get listening IP endpoint
            _logger.Log(Level.Info, "Starting naming server");
            _server = new TransportServer<NamingEvent>(
                new IPEndPoint(NetworkUtils.LocalIPAddress, port), handler, 
                codec, tcpPortProvider);
            _server.Run();
            LocalEndpoint = _server.LocalEndpoint;
        }

        public IPEndPoint LocalEndpoint { get; private set; }

        /// <summary>
        /// Looks up the IPEndpoints for each string identifier
        /// </summary>
        /// <param name="ids">The IDs to look up</param>
        /// <returns>A list of Name assignments representing the identifier
        /// that was searched for and the mapped IPEndpoint</returns>
        public List<NameAssignment> Lookup(List<string> ids)
        {
            if (ids == null)
            {
                Exceptions.Throw(new ArgumentNullException("ids"), _logger);
            }

            return ids.Where(id => _idToAddrMap.ContainsKey(id))
                      .Select(id => new NameAssignment(id, _idToAddrMap[id]))
                      .ToList();
        }

        /// <summary>
        /// Gets all of the registered identifier/endpoint pairs.
        /// </summary>
        /// <returns>A list of all of the registered identifiers and their
        /// mapped IPEndpoints</returns>
        public List<NameAssignment> GetAll()
        {
            return _idToAddrMap.Select(pair => new NameAssignment(pair.Key, pair.Value)).ToList();
        }

        /// <summary>
        /// Registers the string identifier with the given IPEndpoint
        /// </summary>
        /// <param name="id">The string ident</param>
        /// <param name="endpoint">The mapped endpoint</param>
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
            _idToAddrMap[id] = endpoint;
        }

        /// <summary>
        /// Unregister the given identifier with the NameServer
        /// </summary>
        /// <param name="id">The identifier to unregister</param>
        public void Unregister(string id)
        {
            if (id == null)
            {
                Exceptions.Throw(new ArgumentNullException("id"), _logger);
            }

            _logger.Log(Level.Info, "Unregistering id: " + id);
            _idToAddrMap.Remove(id);
        }

        /// <summary>
        /// Stops the NameServer
        /// </summary>
        public void Dispose()
        {
            _server.Dispose();
        }

        /// <summary>
        /// Create the handler to manage incoming NamingEvent types
        /// </summary>
        /// <returns>The server handler</returns>
        private IObserver<TransportEvent<NamingEvent>> CreateServerHandler()
        {
            PubSubSubject<NamingEvent> subject = new PubSubSubject<NamingEvent>();
            subject.Subscribe(new NamingLookupRequestObserver(this));
            subject.Subscribe(new NamingGetAllRequestObserver(this));
            subject.Subscribe(new NamingRegisterRequestObserver(this));
            subject.Subscribe(new NamingUnregisterRequestObserver(this));
            return new ServerHandler(subject);
        }

        /// <summary>
        /// Create the codec used to serialize/deserialize NamingEvent messages
        /// </summary>
        /// <returns>The serialization codec</returns>
        private ICodec<NamingEvent> CreateServerCodec()
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

        [NamedParameter("Port for the NameServer to listen on")]
        public class Port : Name<int>
        {
        }

        /// <summary>
        /// Class used to handle incoming NamingEvent messages.
        /// Delegates the event to the prescribed handler depending on its type
        /// </summary>
        private class ServerHandler : AbstractObserver<TransportEvent<NamingEvent>>
        {
            private readonly IObserver<NamingEvent> _handler; 

            public ServerHandler(IObserver<NamingEvent> handler)
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
