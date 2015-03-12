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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Operators.Impl;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Network.Utilities;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    /// <summary>
    /// Contains the Operator's topology graph.
    /// Used to send or receive messages to/from operators in the same
    /// Communication Group.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public class OperatorTopology<T> : IObserver<GroupCommunicationMessage>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(OperatorTopology<>));

        private readonly string _groupName;
        private readonly string _operatorName;
        private readonly string _selfId;
        private string _driverId;
        private readonly int _timeout;
        private readonly int _retryCount;
        private readonly int _sleepTime;

        private readonly NodeStruct _parent;
        private readonly List<NodeStruct> _children;
        private readonly Dictionary<string, NodeStruct> _idToNodeMap;
        private readonly ICodec<T> _codec;
        private readonly INameClient _nameClient;
        private readonly Sender _sender;
        private readonly BlockingCollection<NodeStruct> _nodesWithData;

        /// <summary>
        /// Initializes a new instance of the <see cref="OperatorTopology{T}"/> class.
        /// </summary>
        /// <param name="operatorName">Name of the operator.</param>
        /// <param name="groupName">Name of the group.</param>
        /// <param name="taskId">The task identifier.</param>
        /// <param name="driverId">The driver identifier.</param>
        /// <param name="timeout">The timeout.</param>
        /// <param name="retryCount">The retry count.</param>
        /// <param name="sleepTime">The sleep time.</param>
        /// <param name="rootId">The root identifier.</param>
        /// <param name="childIds">The child ids.</param>
        /// <param name="networkService">The network service.</param>
        /// <param name="codec">The codec.</param>
        /// <param name="sender">The sender.</param>
        [Inject]
        public OperatorTopology(
            [Parameter(typeof(MpiConfigurationOptions.OperatorName))] string operatorName,
            [Parameter(typeof(MpiConfigurationOptions.CommunicationGroupName))] string groupName,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(MpiConfigurationOptions.DriverId))] string driverId,
            [Parameter(typeof(MpiConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(MpiConfigurationOptions.RetryCount))] int retryCount,
            [Parameter(typeof(MpiConfigurationOptions.SleepTime))] int sleepTime,
            [Parameter(typeof(MpiConfigurationOptions.TopologyRootTaskId))] string rootId,
            [Parameter(typeof(MpiConfigurationOptions.TopologyChildTaskIds))] ISet<string> childIds,
            NetworkService<GroupCommunicationMessage> networkService,
            ICodec<T> codec,
            Sender sender)
        {
            LOGGER.LogFunction("OperatorTopology constructor for operator {0}, task id : {1} ", operatorName, taskId);
            _operatorName = operatorName;
            _groupName = groupName;
            _selfId = taskId;
            _driverId = driverId;
            _timeout = timeout;
            _retryCount = retryCount;
            _sleepTime = sleepTime;
            _codec = codec;
            _nameClient = networkService.NamingClient;
            _sender = sender;
            _nodesWithData = new BlockingCollection<NodeStruct>();
            _children = new List<NodeStruct>();
            _idToNodeMap = new Dictionary<string, NodeStruct>();

            if (_selfId.Equals(rootId))
            {
                _parent = null;
            }
            else
            {
                _parent = new NodeStruct(rootId);
                _idToNodeMap[rootId] = _parent;
            }
            foreach (string childId in childIds)
            {
                NodeStruct node = new NodeStruct(childId);
                _children.Add(node);
                _idToNodeMap[childId] = node;
            }
        }

        /// <summary>
        /// Initializes operator topology.
        /// Waits until all Tasks in the CommunicationGroup have registered themselves
        /// with the Name Service.
        /// </summary>
        public void Initialize()
        {
            using (LOGGER.LogFunction("OperatorTopology::Initialize for operator {0}, id {1} ", _operatorName, _selfId))
            {
                if (_parent != null)
                {
                    WaitForTaskRegistration(_parent.Identifier, _retryCount);
                    LOGGER.Log(Level.Info, "parent registered: {0}.", _parent.Identifier);
                }

                if (_children.Count > 0)
                {
                    foreach (NodeStruct child in _children)
                    {
                        WaitForTaskRegistration(child.Identifier, _retryCount);
                        LOGGER.Log(Level.Info, "child registered: {0}.", child.Identifier);
                    }
                }
            }
            Thread.Sleep(_sleepTime);
        }

        /// <summary>
        /// Handles the incoming GroupCommunicationMessage.
        /// Updates the sending node's message queue.
        /// </summary>
        /// <param name="gcm">The incoming message</param>
        public void OnNext(GroupCommunicationMessage gcm)
        {
            if (gcm == null)
            {
                throw new ArgumentNullException("gcm");
            }
            if (gcm.Source == null)
            {
                throw new ArgumentException("Message must have a source");
            }

            NodeStruct sourceNode = FindNode(gcm.Source);
            if (sourceNode == null)
            {
                throw new IllegalStateException("Received message from invalid task id: " + gcm.Source);
            }

            _nodesWithData.Add(sourceNode);
            sourceNode.AddData(gcm);
        }

        /// <summary>
        /// Sends the message to the parent Task.
        /// </summary>
        /// <param name="message">The message to send</param>
        /// <param name="type">The message type</param>
        public void SendToParent(T message, MessageType type)
        {
            if (_parent == null)
            {
                throw new ArgumentException("No parent for node");
            }

            SendToNode(message, MessageType.Data, _parent);
        }

        /// <summary>
        /// Sends the message to all child Tasks.
        /// </summary>
        /// <param name="message">The message to send</param>
        /// <param name="type">The message type</param>
        public void SendToChildren(T message, MessageType type)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            foreach (NodeStruct child in _children)
            {
                SendToNode(message, MessageType.Data, child); 
            }
        }

        /// <summary>
        /// Splits the list of messages up evenly and sends each sublist
        /// to the child Tasks.
        /// </summary>
        /// <param name="messages">The list of messages to scatter</param>
        /// <param name="type">The message type</param>
        public void ScatterToChildren(List<T> messages, MessageType type)
        {
            if (messages == null)
            {
                throw new ArgumentNullException("messages"); 
            }
            if (_children.Count <= 0)
            {
                return;
            }

            int count = (int) Math.Ceiling(((double) messages.Count) / _children.Count);
            ScatterHelper(messages, _children, count);
        }

        /// <summary>
        /// Splits the list of messages up into chunks of the specified size 
        /// and sends each sublist to the child Tasks.
        /// </summary>
        /// <param name="messages">The list of messages to scatter</param>
        /// <param name="count">The size of each sublist</param>
        /// <param name="type">The message type</param>
        public void ScatterToChildren(List<T> messages, int count, MessageType type)
        {
            if (messages == null)
            {
                throw new ArgumentNullException("messages");
            }
            if (count <= 0)
            {
                throw new ArgumentException("Count must be positive");
            }

            ScatterHelper(messages, _children, count);
        }

        /// <summary>
        /// Splits the list of messages up into chunks of the specified size 
        /// and sends each sublist to the child Tasks in the specified order.
        /// </summary>
        /// <param name="messages">The list of messages to scatter</param>
        /// <param name="order">The order to send messages</param>
        /// <param name="type">The message type</param>
        public void ScatterToChildren(List<T> messages, List<string> order, MessageType type)
        {
            if (messages == null)
            {
                throw new ArgumentNullException("messages");
            }
            if (order == null || order.Count != _children.Count)
            {
                throw new ArgumentException("order cannot be null and must have the same number of elements as child tasks");
            }

            List<NodeStruct> nodes = new List<NodeStruct>(); 
            foreach (string taskId in order)
            {
                NodeStruct node = FindNode(taskId);
                if (node == null)
                {
                    throw new IllegalStateException("Received message from invalid task id: " + taskId);
                }

                nodes.Add(node);
            }

            int count = (int) Math.Ceiling(((double) messages.Count) / _children.Count);
            ScatterHelper(messages, nodes, count);
        }

        /// <summary>
        /// Receive an incoming message from the parent Task.
        /// </summary>
        /// <returns>The parent Task's message</returns>
        public T ReceiveFromParent()
        {
            byte[][] data = ReceiveFromNode(_parent, true);
            if (data == null || data.Length != 1)
            {
                throw new InvalidOperationException("Cannot receive data from parent node");
            }

            return _codec.Decode(data[0]);
        }

        /// <summary>
        /// Receive a list of incoming messages from the parent Task.
        /// </summary>
        /// <returns>The parent Task's list of messages</returns>
        public List<T> ReceiveListFromParent()
        {
            byte[][] data = ReceiveFromNode(_parent, true);
            if (data == null || data.Length == 0)
            {
                throw new InvalidOperationException("Cannot receive data from parent node");
            }

            return data.Select(b => _codec.Decode(b)).ToList();
        }

        /// <summary>
        /// Receives all messages from child Tasks and reduces them with the
        /// given IReduceFunction.
        /// </summary>
        /// <param name="reduceFunction">The class used to reduce messages</param>
        /// <returns>The result of reducing messages</returns>
        public T ReceiveFromChildren(IReduceFunction<T> reduceFunction)
        {
            if (reduceFunction == null)
            {
                throw new ArgumentNullException("reduceFunction");
            }

            var receivedData = new List<T>();
            var childrenToReceiveFrom = new HashSet<string>(_children.Select(node => node.Identifier));

            while (childrenToReceiveFrom.Count > 0)
            {
                NodeStruct childWithData = GetNodeWithData();
                byte[][] data = ReceiveFromNode(childWithData, false);
                if (data == null || data.Length != 1)
                {
                    throw new InvalidOperationException("Received invalid data from child with id: " + childWithData.Identifier);
                }

                receivedData.Add(_codec.Decode(data[0]));
                childrenToReceiveFrom.Remove(childWithData.Identifier);
            }

            return reduceFunction.Reduce(receivedData);
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        public bool HasChildren()
        {
            return _children.Count > 0;
        }

        /// <summary>
        /// Get a node containing an incoming message.
        /// </summary>
        /// <returns>A NodeStruct with incoming data.</returns>
        private NodeStruct GetNodeWithData()
        {
            CancellationTokenSource timeoutSource = new CancellationTokenSource(_timeout);

            try
            {
                return _nodesWithData.Take(timeoutSource.Token);
            }
            catch (OperationCanceledException)
            {
                LOGGER.Log(Level.Error, "No data to read from child");
                throw;
            }
            catch (ObjectDisposedException)
            {
                LOGGER.Log(Level.Error, "No data to read from child");
                throw;
            }
            catch (InvalidOperationException)
            {
                LOGGER.Log(Level.Error, "No data to read from child");
                throw;
            }
        }

        /// <summary>
        /// Sends the message to the Task represented by the given NodeStruct.
        /// </summary>
        /// <param name="message">The message to send</param>
        /// <param name="msgType">The message type</param>
        /// <param name="node">The NodeStruct representing the Task to send to</param>
        private void SendToNode(T message, MessageType msgType, NodeStruct node)
        {
            GroupCommunicationMessage gcm = new GroupCommunicationMessage(_groupName, _operatorName,
                _selfId, node.Identifier, _codec.Encode(message), msgType);

            _sender.Send(gcm);
        }

        /// <summary>
        /// Sends the list of messages to the Task represented by the given NodeStruct.
        /// </summary>
        /// <param name="messages">The list of messages to send</param>
        /// <param name="msgType">The message type</param>
        /// <param name="node">The NodeStruct representing the Task to send to</param>
        private void SendToNode(List<T> messages, MessageType msgType, NodeStruct node)
        {
            byte[][] encodedMessages = messages.Select(message => _codec.Encode(message)).ToArray();
            GroupCommunicationMessage gcm = new GroupCommunicationMessage(_groupName, _operatorName,
                _selfId, node.Identifier, encodedMessages, msgType);

            _sender.Send(gcm);
        }

        private void ScatterHelper(List<T> messages, List<NodeStruct> order, int count)
        {
            if (count <= 0)
            {
                throw new ArgumentException("Count must be positive");
            }

            int i = 0;
            foreach (NodeStruct nodeStruct in order)
            {
                // The last sublist might be smaller than count if the number of
                // child tasks is not evenly divisible by count
                int left = messages.Count - i;
                int size = (left < count) ? left : count;
                if (size <= 0)
                {
                    throw new ArgumentException("Scatter count must be positive");
                }

                List<T> sublist = messages.GetRange(i, size);
                SendToNode(sublist, MessageType.Data, nodeStruct);

                i += size;
            }
        }

        /// <summary>
        /// Receive a message from the Task represented by the given NodeStruct.
        /// Removes the NodeStruct from the nodesWithData queue if requested.
        /// </summary>
        /// <param name="node">The node to receive from</param>
        /// <param name="removeFromQueue">Whether or not to remove the NodeStruct
        /// from the nodesWithData queue</param>
        /// <returns>The byte array message from the node</returns>
        private byte[][] ReceiveFromNode(NodeStruct node, bool removeFromQueue)
        {
            byte[][] data = node.GetData();
            if (removeFromQueue)
            {
                _nodesWithData.Take(node);
            }

            return data;
        }

        /// <summary>
        /// Find the NodeStruct with the given Task identifier.
        /// </summary>
        /// <param name="identifier">The identifier of the Task</param>
        /// <returns>The NodeStruct</returns>
        private NodeStruct FindNode(string identifier)
        {
            NodeStruct node;
            return _idToNodeMap.TryGetValue(identifier, out node) ? node : null;
        }

        /// <summary>
        /// Checks if the identifier is registered with the Name Server.
        /// Throws exception if the operation fails more than the retry count.
        /// </summary>
        /// <param name="identifier">The identifier to look up</param>
        /// <param name="retries">The number of times to retry the lookup operation</param>
        private void WaitForTaskRegistration(string identifier, int retries)
        {
            for (int i = 0; i < retries; i++)
            {
                if (_nameClient.Lookup(identifier) != null)
                {
                    return;
                }
                Thread.Sleep(500);
                LOGGER.Log(Level.Verbose, "Retry {0}: retrying lookup for node: {1}", i + 1, identifier);
            }

            string msg = string.Format(CultureInfo.CurrentCulture,
                "Failed to initialize operator topology for node: {0}", identifier);
            Exceptions.Throw(new IllegalStateException(msg), msg, LOGGER);
        }
    }
}