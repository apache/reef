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

using System.Collections.Concurrent;
using Org.Apache.REEF.Network.Group.Driver.Impl;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    /// <summary>
    /// Stores all incoming messages sent by a particular Task.
    /// Writable version
    /// </summary>
    /// <typeparam name="T"> Generic type of message</typeparam>
    internal sealed class NodeStruct<T>
    {
        private readonly BlockingCollection<GroupCommunicationMessage<T>> _messageQueue;

        /// <summary>
        /// Creates a new NodeStruct.
        /// </summary>
        /// <param name="id">The Task identifier</param>
        /// <param name="groupName">The group name of the node.</param>
        /// <param name="operatorName">The operator name of the node</param>
        internal NodeStruct(string id, string groupName, string operatorName)
        {
            Identifier = id;
            GroupName = groupName;
            OperatorName = operatorName;
            _messageQueue = new BlockingCollection<GroupCommunicationMessage<T>>();
        }

        /// <summary>
        /// Returns the identifier for the Task that sent all
        /// messages in the message queue.
        /// </summary>
        internal string Identifier { get; private set; }

        /// <summary>
        /// The group name of the node.
        /// </summary>
        internal string GroupName { get; private set; }

        /// <summary>
        /// The operator name of the node.
        /// </summary>
        internal string OperatorName { get; private set; }

        /// <summary>
        /// Gets the first message in the message queue.
        /// </summary>
        /// <returns>The first available message.</returns>
        internal T[] GetData()
        {
            return _messageQueue.Take().Data;
        }

        /// <summary>
        /// Adds an incoming message to the message queue.
        /// </summary>
        /// <param name="gcm">The incoming message</param>
        internal void AddData(GroupCommunicationMessage<T> gcm)
        {
            _messageQueue.Add(gcm);
        }
    }
}
