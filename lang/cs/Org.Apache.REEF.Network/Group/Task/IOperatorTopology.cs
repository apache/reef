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

using System.Collections.Generic;
using System.Threading;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Operators;

namespace Org.Apache.REEF.Network.Group.Task
{
    /// <summary>
    /// Contains the Operator's topology graph.
    /// Used to send or receive messages to/from operators in the same
    /// Communication Group.
    /// </summary>
    /// <typeparam name="T">The type of message</typeparam>
    public interface IOperatorTopology<T>
    {
        /// <summary>
        /// Initializes operator topology.
        /// Waits until all Tasks in the CommunicationGroup have registered themselves
        /// with the Name Service.
        /// </summary>
        void Initialize();

        /// <summary>
        /// Sends the message to the parent Task.
        /// </summary>
        /// <param name="message">The message to send</param>
        /// <param name="type">The message type</param>
        void SendToParent(T message, MessageType type);

        /// <summary>
        /// Sends the message to all child Tasks.
        /// </summary>
        /// <param name="message">The message to send</param>
        /// <param name="type">The message type</param>
        void SendToChildren(T message, MessageType type);

        /// <summary>
        /// Splits the list of messages up evenly and sends each sublist
        /// to the child Tasks.
        /// </summary>
        /// <param name="messages">The list of messages to scatter</param>
        /// <param name="type">The message type</param>
        void ScatterToChildren(IList<T> messages, MessageType type);

        /// <summary>
        /// Splits the list of messages up into chunks of the specified size 
        /// and sends each sublist to the child Tasks.
        /// </summary>
        /// <param name="messages">The list of messages to scatter</param>
        /// <param name="count">The size of each sublist</param>
        /// <param name="type">The message type</param>
        void ScatterToChildren(IList<T> messages, int count, MessageType type);

        /// <summary>
        /// Splits the list of messages up into chunks of the specified size 
        /// and sends each sublist to the child Tasks in the specified order.
        /// </summary>
        /// <param name="messages">The list of messages to scatter</param>
        /// <param name="order">The order to send messages</param>
        /// <param name="type">The message type</param>
        void ScatterToChildren(IList<T> messages, List<string> order, MessageType type);

        /// <summary>
        /// Receive an incoming message from the parent Task.
        /// </summary>
        /// <param name="cancellationSource">The cancellation token for the data reading operation cancellation</param>
        /// <returns>The parent Task's message</returns>
        T ReceiveFromParent(CancellationTokenSource cancellationSource = null);

        /// <summary>
        /// Receive a list of incoming messages from the parent Task.
        /// </summary>
        /// <returns>The parent Task's list of messages</returns>
        IList<T> ReceiveListFromParent(CancellationTokenSource cancellationSource = null);

        /// <summary>
        /// Receives all messages from child Tasks and reduces them with the
        /// given IReduceFunction.
        /// </summary>
        /// <param name="reduceFunction">The class used to reduce messages</param>
        /// <param name="cancellationSource">The cancellationSource to cancel the operation</param>
        /// <returns>The result of reducing messages</returns>
        T ReceiveFromChildren(IReduceFunction<T> reduceFunction, CancellationTokenSource cancellationSource = null);

        /// <summary>
        /// Checks if the node has children
        /// </summary>
        /// <returns>true if children are there, false otherwise</returns>
        bool HasChildren();
    }
}
