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
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Network.Naming.Events;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Network.Naming
{
    /// <summary>
    /// Helper class to send lookup events to the name server
    /// </summary>
    internal sealed class NameLookupClient
    {
        private readonly TransportClient<NamingEvent> _client;
        private readonly BlockingCollection<NamingLookupResponse> _lookupResponseQueue;
        private readonly BlockingCollection<NamingGetAllResponse> _getAllResponseQueue;

        /// <summary>
        /// Constructs a new NameLookupClient.
        /// </summary>
        /// <param name="client">The transport client used to connect to the NameServer</param>
        /// <param name="lookupQueue">The queue used to signal that a response
        /// has been received from the NameServer</param>
        /// <param name="getAllQueue">The queue used to signal that a GetAllResponse 
        /// has been received from the NameServer</param>
        public NameLookupClient(TransportClient<NamingEvent> client,
                                BlockingCollection<NamingLookupResponse> lookupQueue,
                                BlockingCollection<NamingGetAllResponse> getAllQueue)
        {
            _client = client;
            _lookupResponseQueue = lookupQueue;
            _getAllResponseQueue = getAllQueue;
        }

        /// <summary>
        /// Look up the IPEndPoint that has been registered with the NameServer using
        /// the given identifier as the key.
        /// </summary>
        /// <param name="id">The id for the IPEndPoint</param>
        /// <param name="token">The cancellation token used for timeout</param>
        /// <returns>The registered IPEndpoint, or null if the identifier has not 
        /// been registered with the NameServer or if the operation times out.</returns>
        public IPEndPoint Lookup(string id, CancellationToken token)
        {
            List<string> ids = new List<string> { id };
            List<NameAssignment> assignment = Lookup(ids);
            return (assignment == null || assignment.Count == 0) ? null : assignment.First().Endpoint;
        }

        /// <summary>
        /// Look up IPEndPoints that have been registered with the NameService
        /// </summary>
        /// <param name="ids">The list of ids to look up</param>
        /// <returns>A list of NameAssignments representing the mapped identifier/IPEndpoint
        /// pairs</returns>
        public List<NameAssignment> Lookup(List<string> ids)
        {
            _client.Send(new NamingLookupRequest(ids));
            NamingLookupResponse response = _lookupResponseQueue.Take();
            return response.NameAssignments;
        }

        /// <summary>
        /// Synchronously gets all of the identifier/IPEndpoint pairs registered with the NameService.
        /// </summary>
        /// <returns>A list of NameAssignments representing the mapped identifier/IPEndpoint
        /// pairs</returns>
        public List<NameAssignment> GetAll()
        {
            _client.Send(new NamingGetAllRequest());
            NamingGetAllResponse response = _getAllResponseQueue.Take();
            return response.Assignments;
        }
    }
}
