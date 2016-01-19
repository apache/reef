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
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake;

namespace Org.Apache.REEF.Network.Group.Operators.Impl
{
    /// <summary>
    /// Group Communication operator used to do point-to-point communication between named Tasks.
    /// It uses Writable classes
    /// </summary>
    internal sealed class Sender
    {
        private readonly INetworkService<GeneralGroupCommunicationMessage> _networkService;
        private readonly IIdentifierFactory _idFactory;

        /// <summary>
        /// Creates a new Sender.
        /// </summary>
        /// <param name="networkService">The network services used to send messages.</param>
        /// <param name="idFactory">Used to create IIdentifier for GroupCommunicationMessages.</param>
        [Inject]
        private Sender(
            StreamingNetworkService<GeneralGroupCommunicationMessage> networkService,
            IIdentifierFactory idFactory)
        {
            _networkService = networkService;
            _idFactory = idFactory;
        }

        /// <summary>
        /// Send the GroupCommunicationMessage to the Task whose name is
        /// included in the message.
        /// </summary>
        /// <param name="message">The message to send.</param>
        internal void Send(GeneralGroupCommunicationMessage message)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }
            if (string.IsNullOrEmpty(message.Destination))
            {
                throw new ArgumentException("Message destination cannot be null or empty");
            }

            IIdentifier destId = _idFactory.Create(message.Destination);
            var conn = _networkService.NewConnection(destId);
            conn.Open();
            conn.Write(message);
        }
    }
}
