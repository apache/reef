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

namespace Org.Apache.REEF.Common.Io
{
    /// <summary>
    /// Client for the Reef name service. 
    /// Used to register, unregister, and lookup IP Addresses of known hosts.
    /// </summary>
    public interface INameClient : IDisposable
    {
        /// <summary>
        /// Registers the identifier with the NameService.  
        /// Overwrites the previous mapping if the identifier has already 
        /// been registered.
        /// </summary>
        /// <param name="id">The key used to map the remote endpoint</param>
        /// <param name="endpoint">The endpoint to map</param>
        void Register(string id, IPEndPoint endpoint);

        /// <summary>
        /// Unregisters the remote identifier with the NameService
        /// </summary>
        /// <param name="id">The identifier to unregister</param>
        void Unregister(string id);

        /// <summary>
        /// Looks up the IPEndpoint for the registered identifier.
        /// </summary>
        /// <param name="id">The identifier to look up</param>
        /// <returns>The mapped IPEndpoint for the identifier, or null if
        /// the identifier has not been registered with the NameService</returns>
        IPEndPoint Lookup(string id);

        /// <summary>
        /// Looks up the IPEndpoint for the registered identifier.
        /// Use cache if it has entry
        /// </summary>
        /// <param name="id">The identifier to look up</param>
        /// <returns>The mapped IPEndpoint for the identifier, or null if
        /// the identifier has not been registered with the NameService</returns>
        IPEndPoint CacheLookup(string id);

        /// <summary>
        /// Looks up the IPEndpoint for each of the registered identifiers in the list.
        /// </summary>
        /// <param name="ids">The list of identifiers to look up</param>
        /// <returns>The list of NameAssignments representing a pair of identifer
        /// and mapped IPEndpoint for that identifier.  If any of the requested identifiers
        /// are not registered with the NameService, their corresponding NameAssignment
        /// IPEndpoint value will be null.</returns>
        List<NameAssignment> Lookup(List<string> ids);

        /// <summary>
        /// Restart the name client in case of failure.
        /// </summary>
        /// <param name="serverEndpoint">The new server endpoint to connect to</param>
        void Restart(IPEndPoint serverEndpoint);
    }
}
