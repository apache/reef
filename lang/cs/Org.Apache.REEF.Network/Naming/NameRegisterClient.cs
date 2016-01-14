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

using System.Collections.Concurrent;
using System.Net;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Network.Naming.Events;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Network.Naming
{
    /// <summary>
    /// Helper class to send register and unregister events to the NameServer.
    /// </summary>
    internal sealed class NameRegisterClient
    {
        private readonly TransportClient<NamingEvent> _client;
        private readonly BlockingCollection<NamingRegisterResponse> _registerResponseQueue;
        private BlockingCollection<NamingUnregisterResponse> _unregisterResponseQueue;

        public NameRegisterClient(TransportClient<NamingEvent> client,
                                  BlockingCollection<NamingRegisterResponse> registerQueue,
                                  BlockingCollection<NamingUnregisterResponse> unregisterQueue)
        {
            _client = client;
            _registerResponseQueue = registerQueue;
            _unregisterResponseQueue = unregisterQueue;
        }

        /// <summary>
        /// Synchronously register the id and endpoint with the NameServer.
        /// </summary>
        /// <param name="id">The identifier</param>
        /// <param name="endpoint">The endpoint</param>
        public void Register(string id, IPEndPoint endpoint)
        {
            NameAssignment assignment = new NameAssignment(id, endpoint);
            _client.Send(new NamingRegisterRequest(assignment));
            _registerResponseQueue.Take();
        }

        /// <summary>
        /// Synchronously unregisters the identifier with the NameServer.
        /// </summary>
        /// <param name="id">The identifier to unregister</param>
        public void Unregister(string id)
        {
            _client.Send(new NamingUnregisterRequest(id));
        }
    }
}
