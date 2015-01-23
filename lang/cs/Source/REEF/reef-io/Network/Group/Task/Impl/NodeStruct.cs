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

using Org.Apache.Reef.IO.Network.Group.Driver;
using System.Collections.Concurrent;
using Org.Apache.Reef.IO.Network.Group.Driver.Impl;

namespace Org.Apache.Reef.IO.Network.Group.Task.Impl
{
    /// <summary>
    /// Stores all incoming messages sent by a particular Task.
    /// </summary>
    internal class NodeStruct
    {
        private BlockingCollection<GroupCommunicationMessage> _messageQueue;

        /// <summary>
        /// Creates a new NodeStruct.
        /// </summary>
        /// <param name="id">The Task identifier</param>
        public NodeStruct(string id)
        {
            Identifier = id;
            _messageQueue = new BlockingCollection<GroupCommunicationMessage>();
        }

        /// <summary>
        /// Returns the identifier for the Task that sent all
        /// messages in the message queue.
        /// </summary>
        public string Identifier { get; private set; }

        /// <summary>
        /// Gets the first message in the message queue.
        /// </summary>
        /// <returns>The first available message.</returns>
        public byte[][] GetData()
        {
            return _messageQueue.Take().Data;
        }

        /// <summary>
        /// Adds an incoming message to the message queue.
        /// </summary>
        /// <param name="gcm">The incoming message</param>
        public void AddData(GroupCommunicationMessage gcm)
        {
            _messageQueue.Add(gcm);
        }
    }
}
