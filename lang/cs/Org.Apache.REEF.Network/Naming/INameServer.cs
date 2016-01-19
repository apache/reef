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
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Network.Naming
{
    /// <summary>
    /// Service that manages names and IPEndpoints for well known hosts.
    /// Can register, unregister, and look up IPAddresses using a string identifier.
    /// </summary>
    [DefaultImplementation(typeof(NameServer))]
    public interface INameServer : IDisposable
    {
        /// <summary>
        /// Listening endpoint for the NameServer
        /// </summary>
        IPEndPoint LocalEndpoint { get; }

        /// <summary>
        /// Looks up the IPEndpoints for each string identifier
        /// </summary>
        /// <param name="ids">The IDs to look up</param>
        /// <returns>A list of Name assignments representing the identifier
        /// that was searched for and the mapped IPEndpoint</returns>
        List<NameAssignment> Lookup(List<string> ids);

        /// <summary>
        /// Gets all of the registered identifier/endpoint pairs.
        /// </summary>
        /// <returns>A list of all of the registered identifiers and their
        /// mapped IPEndpoints</returns>
        List<NameAssignment> GetAll();

        /// <summary>
        /// Registers the string identifier with the given IPEndpoint
        /// </summary>
        /// <param name="id">The string ident</param>
        /// <param name="endpoint">The mapped endpoint</param>
        void Register(string id, IPEndPoint endpoint);

        /// <summary>
        /// Unregister the given identifier with the NameServer
        /// </summary>
        /// <param name="id">The identifier to unregister</param>
        void Unregister(string id);
    }
}
