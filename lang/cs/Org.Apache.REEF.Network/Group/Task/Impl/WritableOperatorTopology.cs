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
using System.Linq;
using System.Threading;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Operators.Impl;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Network.StreamingCodec;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    /// <summary>
    /// Contains the Operator's topology graph.
    /// Writable version
    /// Used to send or receive messages to/from operators in the same
    /// Communication Group.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    [Obsolete("Need to remove Iwritable and use IstreamingCodec. Please see Jira REEF-295 ", false)]
    public class WritableOperatorTopology<T> : IOperatorTopology<T>, IObserver<WritableGeneralGroupCommunicationMessage>
    {
        private const int DefaultTimeout = 50000;
        private const int RetryCount = 10;

        private static readonly Logger Logger = Logger.GetLogger(typeof(OperatorTopology<>));

        private readonly string _groupName;
        private readonly string _operatorName;
        private readonly string _selfId;
        private string _driverId;
        private readonly int _timeout;
        private readonly int _retryCount;

        private readonly WritableNodeStruct<T> _parent;
        private readonly List<WritableNodeStruct<T>> _children;
        private readonly Dictionary<string, WritableNodeStruct<T>> _idToNodeMap;
        private readonly INameClient _nameClient;
        private readonly WritableSender _sender;
        private readonly BlockingCollection<WritableNodeStruct<T>> _nodesWithData;
        private readonly Object _thisLock = new Object();
        private readonly IStreamingCodec<T> _codec;

        /// <summary>
        /// Creates a new OperatorTopology object.
        /// </summary>
        /// <param name="operatorName">The name of the Group Communication Operator</param>
        /// <param name="groupName">The name of the operator's Communication Group</param>
        /// <param name="taskId">The operator's Task identifier</param>
        /// <param name="driverId">The identifer for the driver</param>
        /// <param name="timeout">Timeout value for cancellation token</param>
        /// <param name="retryCount">Number of times to retry registration</param>
        /// <param name="rootId">The identifier for the root Task in the topology graph</param>
        /// <param name="childIds">The set of child Task identifiers in the topology graph</param>
        /// <param name="networkService">The network service</param>
        /// <param name="sender">The Sender used to do point to point communication</param>
        /// <param name="codec">Streaming codec to encode objects</param>
        [Inject]
        private WritableOperatorTopology(
            [Parameter(typeof(GroupCommConfigurationOptions.OperatorName))] string operatorName,
            [Parameter(typeof(GroupCommConfigurationOptions.CommunicationGroupName))] string groupName,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(GroupCommConfigurationOptions.DriverId))] string driverId,
            [Parameter(typeof(GroupCommConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(GroupCommConfigurationOptions.RetryCount))] int retryCount,
            [Parameter(typeof(GroupCommConfigurationOptions.TopologyRootTaskId))] string rootId,
            [Parameter(typeof(GroupCommConfigurationOptions.TopologyChildTaskIds))] ISet<string> childIds,
            WritableNetworkService<WritableGeneralGroupCommunicationMessage> networkService,
            WritableSender sender,
            IStreamingCodec<T> codec)
        {
            _operatorName = operatorName;
            _groupName = groupName;
            _selfId = taskId;
            _driverId = driverId;
            _timeout = timeout;
            _retryCount = retryCount;
            _nameClient = networkService.NamingClient;
            _sender = sender;
            _nodesWithData = new BlockingCollection<WritableNodeStruct<T>>();
            _children = new List<WritableNodeStruct<T>>();
            _idToNodeMap = new Dictionary<string, WritableNodeStruct<T>>();
            _codec = codec;

            if (_selfId.Equals(rootId))
            {
                _parent = null;
            }
            else
            {
                _parent = new WritableNodeStruct<T>(rootId);
                _idToNodeMap[rootId] = _parent;
            }
            foreach (var childId in childIds)
            {
                var node = new WritableNodeStruct<T>(childId);
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
            using (Logger.LogFunction("OperatorTopology::Initialize"))
            {
                if (_parent != null)
                {
                    WaitForTaskRegistration(_parent.Identifier, _retryCount);
                }

                if (_children.Count > 0)
                {
                    foreach (var child in _children)
                    {
                        WaitForTaskRegistration(child.Identifier, _retryCount);
                    }
                }
            }
        }

        /// <summary>
        /// Handles the incoming GroupCommunicationMessage.
        /// Updates the sending node's message queue.
        /// </summary>
        /// <param name="gcm">The incoming message</param>
        public void OnNext(WritableGeneralGroupCommunicationMessage gcm)
        {
            if (gcm == null)
            {
                throw new ArgumentNullException("gcm");
            }
            if (gcm.Source == null)
            {
                throw new ArgumentException("Message must have a source");
            }

            var sourceNode = FindNode(gcm.Source);
            if (sourceNode == null)
            {
                throw new IllegalStateException("Received message from invalid task id: " + gcm.Source);
            }

            lock (_thisLock)
            {
                _nodesWithData.Add(sourceNode);
                sourceNode.AddData(gcm as WritableGroupCommunicationMessage<T>);
            }
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

            foreach (var child in _children)
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
        public void ScatterToChildren(IList<T> messages, MessageType type)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Splits the list of messages up into chunks of the specified size 
        /// and sends each sublist to the child Tasks.
        /// </summary>
        /// <param name="messages">The list of messages to scatter</param>
        /// <param name="count">The size of each sublist</param>
        /// <param name="type">The message type</param>
        public void ScatterToChildren(IList<T> messages, int count, MessageType type)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Splits the list of messages up into chunks of the specified size 
        /// and sends each sublist to the child Tasks in the specified order.
        /// </summary>
        /// <param name="messages">The list of messages to scatter</param>
        /// <param name="order">The order to send messages</param>
        /// <param name="type">The message type</param>
        public void ScatterToChildren(IList<T> messages, List<string> order, MessageType type)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Receive an incoming message from the parent Task.
        /// </summary>
        /// <returns>The parent Task's message</returns>
        public T ReceiveFromParent()
        {
            T[] data = ReceiveFromNode(_parent);
            if (data == null || data.Length != 1)
            {
                throw new InvalidOperationException("Cannot receive data from parent node");
            }

            return data[0];
        }

        public IList<T> ReceiveListFromParent()
        {
            throw new NotImplementedException();
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
                var childrenWithData = GetNodeWithData(childrenToReceiveFrom);

                foreach (var child in childrenWithData)
                {
                    T[] data = ReceiveFromNode(child);
                    if (data == null || data.Length != 1)
                    {
                        throw new InvalidOperationException("Received invalid data from child with id: " + child.Identifier);
                    }

                    receivedData.Add(data[0]);
                    childrenToReceiveFrom.Remove(child.Identifier);
                }
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
        /// Get a set of nodes containing an incoming message and belonging to candidate set of nodes.
        /// </summary>
        ///<param name="nodeSetIdentifier">Candidate set of nodes from which data is to be received</param>
        /// <returns>A Vector of NodeStruct with incoming data.</returns>
        private IEnumerable<WritableNodeStruct<T>> GetNodeWithData(IEnumerable<string> nodeSetIdentifier)
        {
            CancellationTokenSource timeoutSource = new CancellationTokenSource(_timeout);
            List<WritableNodeStruct<T>> nodesSubsetWithData = new List<WritableNodeStruct<T>>();

            try
            {
                lock (_thisLock)
                {
                    foreach (var identifier in nodeSetIdentifier)
                    {
                        if (!_idToNodeMap.ContainsKey(identifier))
                        {
                            throw new Exception("Trying to get data from the node not present in the node map");
                        }

                        if (_idToNodeMap[identifier].HasMessage())
                        {
                            nodesSubsetWithData.Add(_idToNodeMap[identifier]);
                        }
                    }

                    if (nodesSubsetWithData.Count > 0)
                    {
                        return nodesSubsetWithData;
                    }

                    while (_nodesWithData.Count != 0)
                    {
                        _nodesWithData.Take();
                    }
                }

                var potentialNode = _nodesWithData.Take();

                while (!nodeSetIdentifier.Contains(potentialNode.Identifier))
                {
                    potentialNode = _nodesWithData.Take();
                }

                return new WritableNodeStruct<T>[] { potentialNode };

            }
            catch (OperationCanceledException)
            {
                Logger.Log(Level.Error, "No data to read from child");
                throw;
            }
            catch (ObjectDisposedException)
            {
                Logger.Log(Level.Error, "No data to read from child");
                throw;
            }
            catch (InvalidOperationException)
            {
                Logger.Log(Level.Error, "No data to read from child");
                throw;
            }
        }

        /// <summary>
        /// Sends the message to the Task represented by the given NodeStruct.
        /// </summary>
        /// <param name="message">The message to send</param>
        /// <param name="msgType">The message type</param>
        /// <param name="node">The NodeStruct representing the Task to send to</param>
        private void SendToNode(T message, MessageType msgType, WritableNodeStruct<T> node)
        {
            WritableGeneralGroupCommunicationMessage gcm = new WritableGroupCommunicationMessage<T>(_groupName, _operatorName,
                _selfId, node.Identifier, message, msgType, _codec);

            _sender.Send(gcm);
        }

        /// <summary>
        /// Receive a message from the Task represented by the given NodeStruct.
        /// Removes the NodeStruct from the nodesWithData queue if requested.
        /// </summary>
        /// <param name="node">The node to receive from</param>
        /// <returns>The byte array message from the node</returns>
        private T[] ReceiveFromNode(WritableNodeStruct<T> node)
        {
            var data = node.GetData();
            return data;
        }

        /// <summary>
        /// Find the NodeStruct with the given Task identifier.
        /// </summary>
        /// <param name="identifier">The identifier of the Task</param>
        /// <returns>The NodeStruct</returns>
        private WritableNodeStruct<T> FindNode(string identifier)
        {
            WritableNodeStruct<T> node;
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
                System.Net.IPEndPoint endPoint;
                if ((endPoint = _nameClient.Lookup(identifier)) != null)
                {
                    return;
                }

                Thread.Sleep(500);
            }

            throw new IllegalStateException("Failed to initialize operator topology for node: " + identifier);
        }
    }
}
