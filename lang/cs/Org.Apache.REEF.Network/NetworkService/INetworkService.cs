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
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Wake;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.NetworkService
{
    /// <summary>
    /// Network service used for Reef Task communication.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public interface INetworkService<T> : IDisposable
    {
        /// <summary>
        /// Name client for registering ids
        /// </summary>
        INameClient NamingClient { get; }

        /// <summary>
        /// The remote manager of the NetworkService.
        /// </summary>
        IRemoteManager<NsMessage<T>> RemoteManager { get; }

            /// <summary>
        /// Open a new connection to the remote host registered to
        /// the name service with the given identifier
        /// </summary>
        /// <param name="destinationId">The identifier of the remote host</param>
        /// <returns>The IConnection used for communication</returns>
        IConnection<T> NewConnection(IIdentifier destinationId);

        /// <summary>
        /// Register the identifier for the NetworkService with the NameService.
        /// </summary>
        /// <param name="id">The identifier to register</param>
        void Register(IIdentifier id);

        /// <summary>
        /// Unregister the identifier for the NetworkService with the NameService.
        /// </summary>
        void Unregister();
    }
}
