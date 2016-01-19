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
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Remoting;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.NetworkService
{
    /// <summary>
    /// Represents a connection between two hosts using the NetworkService.
    /// </summary>
    public class NsConnection<T> : IConnection<T>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(NsConnection<T>));

        private readonly IIdentifier _sourceId;
        private readonly IIdentifier _destId;
        private readonly INameClient _nameClient;
        private readonly IRemoteManager<NsMessage<T>> _remoteManager; 
        private readonly Dictionary<IIdentifier, IConnection<T>> _connectionMap;
        private IObserver<NsMessage<T>> _remoteSender;

        /// <summary>
        /// Creates a new NsConnection between two hosts.
        /// </summary>
        /// <param name="sourceId">The identifier of the sender</param>
        /// <param name="destId">The identifier of the receiver</param>
        /// <param name="nameClient">The NameClient used for naming lookup</param>
        /// <param name="remoteManager">The remote manager used for network communication</param>
        /// <param name="connectionMap">A cache of opened connections.  Will remove itself from
        /// the cache when the NsConnection is disposed.</param>
        public NsConnection(
            IIdentifier sourceId, 
            IIdentifier destId, 
            INameClient nameClient,
            IRemoteManager<NsMessage<T>> remoteManager,
            Dictionary<IIdentifier, IConnection<T>> connectionMap)
        {
            _sourceId = sourceId;
            _destId = destId;
            _nameClient = nameClient;
            _remoteManager = remoteManager;
            _connectionMap = connectionMap;
        }

        /// <summary>
        /// Opens the connection to the remote host.
        /// </summary>
        public void Open()
        {
            string destStr = _destId.ToString();
            LOGGER.Log(Level.Verbose, "Network service opening connection to {0}...", destStr);

            IPEndPoint destAddr = _nameClient.CacheLookup(_destId.ToString());
            if (destAddr == null)
            {
                throw new RemotingException("Cannot register Identifier with NameService");
            }

            try
            {
                _remoteSender = _remoteManager.GetRemoteObserver(destAddr);
                LOGGER.Log(Level.Verbose, "Network service completed connection to {0}.", destStr);
            }
            catch (SocketException)
            {
                LOGGER.Log(Level.Error, "Network Service cannot open connection to " + destAddr);
                throw;
            }
            catch (ObjectDisposedException)
            {
                LOGGER.Log(Level.Error, "Network Service cannot open connection to " + destAddr);
                throw;
            }
        }

        /// <summary>
        /// Writes the object to the remote host.
        /// </summary>
        /// <param name="message">The message to send</param>
        public void Write(T message)
        {
            if (_remoteSender == null)
            {
                throw new IllegalStateException("NsConnection has not been opened yet."); 
            }

            try
            {
                _remoteSender.OnNext(new NsMessage<T>(_sourceId, _destId, message));
            }
            catch (IOException)
            {
                LOGGER.Log(Level.Error, "Network Service cannot write message to {0}", _destId);
                throw;
            }
            catch (ObjectDisposedException)
            {
                LOGGER.Log(Level.Error, "Network Service cannot write message to {0}", _destId);
                throw;
            }
        }

        /// <summary>
        /// Closes the connection
        /// </summary>
        public void Dispose()
        {
            _connectionMap.Remove(_destId);
        }
    }
}
