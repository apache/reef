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
using System.Linq;
using System.Threading;
using Org.Apache.REEF.Common.Exceptions;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Operators.Impl;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities.Diagnostics;
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
    public sealed class OperatorTopology<T> : IOperatorTopology<T>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(OperatorTopology<>));

        private readonly string _groupName;
        private readonly string _operatorName;
        private readonly string _selfId;
        private readonly int _timeout;
        private readonly int _retryCount;
        private readonly int _sleepTime;

        private readonly ChildNodeContainer<T> _childNodeContainer = new ChildNodeContainer<T>();
        private readonly NodeStruct<T> _parent;
        private readonly INameClient _nameClient;
        private readonly Sender _sender;

        /// <summary>
        /// Creates a new OperatorTopology object.
        /// </summary>
        /// <param name="operatorName">The name of the Group Communication Operator</param>
        /// <param name="groupName">The name of the operator's Communication Group</param>
        /// <param name="taskId">The operator's Task identifier</param>
        /// <param name="timeout">Timeout value for cancellation token</param>
        /// <param name="retryCount">Number of times to retry waiting for registration</param>
        /// <param name="sleepTime">Sleep time between retry waiting for registration</param>
        /// <param name="rootId">The identifier for the root Task in the topology graph</param>
        /// <param name="childIds">The set of child Task identifiers in the topology graph</param>
        /// <param name="networkObserver"></param>
        /// <param name="networkService">The network service</param>
        /// <param name="sender">The Sender used to do point to point communication</param>
        [Inject]
        private OperatorTopology(
            [Parameter(typeof(GroupCommConfigurationOptions.OperatorName))] string operatorName,
            [Parameter(typeof(GroupCommConfigurationOptions.CommunicationGroupName))] string groupName,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(GroupCommConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(GroupCommConfigurationOptions.RetryCountWaitingForRegistration))] int retryCount,
            [Parameter(typeof(GroupCommConfigurationOptions.SleepTimeWaitingForRegistration))] int sleepTime,
            [Parameter(typeof(GroupCommConfigurationOptions.TopologyRootTaskId))] string rootId,
            [Parameter(typeof(GroupCommConfigurationOptions.TopologyChildTaskIds))] ISet<string> childIds,
            GroupCommNetworkObserver networkObserver,
            StreamingNetworkService<GeneralGroupCommunicationMessage> networkService,
            Sender sender)
        {
            _operatorName = operatorName;
            _groupName = groupName;
            _selfId = taskId;
            _timeout = timeout;
            _retryCount = retryCount;
            _sleepTime = sleepTime;
            _nameClient = networkService.NamingClient;
            _sender = sender;
            _parent = _selfId.Equals(rootId) ? null : new NodeStruct<T>(rootId, groupName, operatorName);

            // Register the observers for Task IDs and nodes adjacent to the current node
            // in the group communication graph.
            if (_parent != null)
            {
                networkObserver.RegisterAndGetForTask(_parent.Identifier).RegisterNodeObserver(new NodeMessageObserver<T>(_parent));
            }

            foreach (var childId in childIds)
            {
                var childNode = new NodeStruct<T>(childId, groupName, operatorName);
                _childNodeContainer.PutNode(childNode);
                networkObserver.RegisterAndGetForTask(childId).RegisterNodeObserver(new NodeMessageObserver<T>(childNode));
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
                IList<string> idsToWait = new List<string>();

                if (_parent != null)
                {
                    idsToWait.Add(_parent.Identifier);
                }

                if (_childNodeContainer.Count > 0)
                {
                    foreach (var child in _childNodeContainer)
                    {
                        idsToWait.Add(child.Identifier);
                    }
                }
                WaitForTaskRegistration(idsToWait);
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

            SendToNode(message, _parent);
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

            foreach (var child in _childNodeContainer)
            {
                SendToNode(message, child);
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
            if (messages == null)
            {
                throw new ArgumentNullException("messages");
            }
            if (_childNodeContainer.Count <= 0)
            {
                return;
            }

            var numMessagesPerChild = (int)Math.Ceiling(((double)messages.Count) / _childNodeContainer.Count);
            ScatterHelper(messages, _childNodeContainer.ToList(), numMessagesPerChild);
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
            if (messages == null)
            {
                throw new ArgumentNullException("messages");
            }
            if (count <= 0)
            {
                throw new ArgumentException("Count must be positive");
            }

            ScatterHelper(messages, _childNodeContainer.ToList(), count);
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
            if (messages == null)
            {
                throw new ArgumentNullException("messages");
            }
            if (order == null || order.Count != _childNodeContainer.Count)
            {
                throw new ArgumentException("order cannot be null and must have the same number of elements as child tasks");
            }

            List<NodeStruct<T>> nodes = new List<NodeStruct<T>>();
            foreach (string taskId in order)
            {
                NodeStruct<T> node;
                if (!_childNodeContainer.TryGetChild(taskId, out node))
                {
                    throw new IllegalStateException("Received message from invalid task id: " + taskId);
                }

                nodes.Add(node);
            }

            int numMessagesPerChild = (int)Math.Ceiling(((double)messages.Count) / _childNodeContainer.Count);
            ScatterHelper(messages, nodes, numMessagesPerChild);
        }

        /// <summary>
        /// Receive an incoming message from the parent Task.
        /// </summary>
        /// <param name="cancellationSource">The cancellation token for the data reading operation cancellation</param>
        /// <returns>The parent Task's message</returns>
        public T ReceiveFromParent(CancellationTokenSource cancellationSource = null)
        {
            T[] data = _parent.GetData(cancellationSource);
            if (data == null || data.Length != 1)
            {
                throw new InvalidOperationException("Cannot receive data from parent node");
            }

            return data[0];
        }

        /// <summary>
        /// Receive a list of incoming messages from the parent Task.
        /// </summary>
        /// <param name="cancellationSource">The cancellation token for the data reading operation cancellation</param>
        /// <returns></returns>
        public IList<T> ReceiveListFromParent(CancellationTokenSource cancellationSource = null)
        {
            T[] data = _parent.GetData(cancellationSource);
            if (data == null || data.Length == 0)
            {
                throw new InvalidOperationException("Cannot receive data from parent node");
            }

            return data.ToList();
        }

        /// <summary>
        /// Receives all messages from child Tasks and reduces them with the
        /// given IReduceFunction.
        /// </summary>
        /// <param name="reduceFunction">The class used to reduce messages</param>
        /// <param name="cancellationSource">The cancellation token for the data reading operation cancellation</param>
        /// <returns>The result of reducing messages</returns>
        public T ReceiveFromChildren(IReduceFunction<T> reduceFunction, CancellationTokenSource cancellationSource = null)
        {
            if (reduceFunction == null)
            {
                throw new ArgumentNullException("reduceFunction");
            }

            return reduceFunction.Reduce(_childNodeContainer.GetDataFromAllChildren(cancellationSource));
        }

        public bool HasChildren()
        {
            return _childNodeContainer.Count > 0;
        }

        /// <summary>
        /// Sends the message to the Task represented by the given NodeStruct.
        /// </summary>
        /// <param name="message">The message to send</param>
        /// <param name="node">The NodeStruct representing the Task to send to</param>
        private void SendToNode(T message, NodeStruct<T> node)
        {
            GeneralGroupCommunicationMessage gcm = new GroupCommunicationMessage<T>(_groupName, _operatorName,
                _selfId, node.Identifier, message);
            _sender.Send(gcm);
        }

        /// <summary>
        /// Sends the list of messages to the Task represented by the given NodeStruct.
        /// </summary>
        /// <param name="messages">The list of messages to send</param>
        /// <param name="node">The NodeStruct representing the Task to send to</param>
        private void SendToNode(IList<T> messages, NodeStruct<T> node)
        {
            T[] encodedMessages = messages.ToArray();

            GroupCommunicationMessage<T> gcm = new GroupCommunicationMessage<T>(_groupName, _operatorName,
                _selfId, node.Identifier, encodedMessages);

            _sender.Send(gcm);
        }

        private void ScatterHelper(IList<T> messages, IList<NodeStruct<T>> order, int numMessagesPerChild)
        {
            if (numMessagesPerChild <= 0)
            {
                throw new ArgumentException("Count must be positive");
            }

            int numMessagesSent = 0;
            foreach (var nodeStruct in order)
            {
                // The last sublist might be smaller than count if the number of
                // child tasks is not evenly divisible by count
                int numMessagesLeft = messages.Count - numMessagesSent;
                int numMessagesToSend = (numMessagesLeft < numMessagesPerChild) ? numMessagesLeft : numMessagesPerChild;
                if (numMessagesToSend <= 0)
                {
                    throw new ArgumentException("Scatter count must be positive");
                }

                IList<T> sublist = messages.ToList().GetRange(numMessagesSent, numMessagesToSend);
                SendToNode(sublist, nodeStruct);

                numMessagesSent += numMessagesToSend;
            }
        }

        /// <summary>
        /// Checks if the identifier is registered with the Name Server.
        /// Throws exception if the operation fails more than the retry count.
        /// </summary>
        /// <param name="identifiers">The identifier to look up</param>
        private void WaitForTaskRegistration(IList<string> identifiers)
        {
            using (Logger.LogFunction("OperatorTopology::WaitForTaskRegistration"))
            {
                IList<string> foundList = new List<string>();
                try
                {
                    for (var i = 0; i < _retryCount; i++)
                    {
                        Logger.Log(Level.Info, "OperatorTopology.WaitForTaskRegistration, in retryCount {0}.", i);
                        foreach (var identifier in identifiers)
                        {
                            if (!foundList.Contains(identifier) && _nameClient.Lookup(identifier) != null)
                            {
                                foundList.Add(identifier);
                                Logger.Log(Level.Verbose, "OperatorTopology.WaitForTaskRegistration, find a dependent id {0} at loop {1}.", identifier, i);
                            }
                        }

                        if (foundList.Count == identifiers.Count)
                        {
                            Logger.Log(Level.Info, "OperatorTopology.WaitForTaskRegistration, found all {0} dependent ids at loop {1}.", foundList.Count, i);
                            return;
                        }

                        Thread.Sleep(_sleepTime);
                    }
                }
                catch (Exception e)
                {
                    Exceptions.CaughtAndThrow(e, Level.Error, "Exception in OperatorTopology.WaitForTaskRegistration.", Logger);
                }

                var leftOver = string.Join(",", identifiers.Where(e => !foundList.Contains(e)));
                Logger.Log(Level.Error, "For node {0}, cannot find registered parent/children: {1}.", _selfId, leftOver);
                Exceptions.Throw(new ReefRuntimeException("Failed to initialize operator topology for node: " + _selfId), Logger);
            }
        }
    }
}