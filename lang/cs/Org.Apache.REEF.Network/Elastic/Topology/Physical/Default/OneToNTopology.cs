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

using Org.Apache.REEF.Network.Elastic.Task.Impl;
using System.Collections.Generic;
using System;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Tang.Exceptions;
using System.Threading;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.NetworkService;
using System.Collections.Concurrent;
using System.Linq;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Default
{
    /// <summary>
    /// Base class for topologies following a one to N communication pattern.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal abstract class OneToNTopology : OperatorTopologyWithDefaultCommunication
    {
        protected static readonly Logger Log = Logger.GetLogger(typeof(OneToNTopology));

        protected readonly ConcurrentDictionary<string, byte> _nodesToRemove =
            new ConcurrentDictionary<string, byte>();

        protected readonly ManualResetEvent _topologyUpdateReceived;
        protected readonly bool _piggybackTopologyUpdates;

        /// <summary>
        /// Construct a one to N topology.
        /// </summary>
        /// <param name="stageName">The stage name the topology is working on</param>
        /// <param name="taskId">The identifier of the task the topology is running on</param>
        /// <param name="rootTaskId">The identifier of the root note in the topology</param>
        /// <param name="operatorId">The identifier of the operator for this topology</param>
        /// <param name="children">The list of nodes this task has to send messages to</param>
        /// <param name="piggyback">Whether to piggyback topology update messages to data message</param>
        /// <param name="retry">How many times the topology will retry to send a message</param>
        /// <param name="timeout">After how long the topology waits for an event</param>
        /// <param name="disposeTimeout">Maximum wait time for topology disposal</param>
        /// <param name="commLayer">Layer responsible for communication</param>
        /// <param name="checkpointLayer">Layer responsible for saving and retrieving checkpoints</param>
        public OneToNTopology(
            string stageName,
            string taskId,
            string rootTaskId,
            int operatorId,
            ISet<int> children,
            bool piggyback,
            int retry,
            int timeout,
            int disposeTimeout,
            DefaultCommunicationLayer commLayer) : base(
                stageName,
                taskId,
                rootTaskId,
                operatorId,
                commLayer,
                retry,
                timeout,
                disposeTimeout)
        {
            _topologyUpdateReceived = new ManualResetEvent(RootTaskId != taskId);

            _commLayer.RegisterOperatorTopologyForTask(this);
            _commLayer.RegisterOperatorTopologyForDriver(this);

            _piggybackTopologyUpdates = piggyback;

            foreach (var child in children)
            {
                var childTaskId = Utils.BuildTaskId(StageName, child);

                _children.TryAdd(child, childTaskId);
            }
        }

        /// <summary>
        /// Whether the topology is still sending messages or not.
        /// </summary>
        public bool IsSending
        {
            get { return !_sendQueue.IsEmpty; }
        }

        /// <summary>
        /// Waiting logic before disposing topologies.
        /// </summary>
        public void WaitCompletionBeforeDisposing(CancellationTokenSource cancellationSource)
        {
            if (TaskId == RootTaskId)
            {
                foreach (var node in _children.Values)
                {
                    while (_commLayer.Lookup(node) && !cancellationSource.IsCancellationRequested)
                    {
                        Thread.Sleep(100);
                    }
                }
            }
        }

        /// <summary>
        /// Creates a DataMessage out of some input data.
        /// </summary>
        /// <param name="iteration">The iteration number of this message</param>
        /// <param name="data">The data to communicate</param>
        /// <returns>A properly configured DataMessage</returns>
        public abstract DataMessage GetDataMessage<T>(int iteration, T data);

        /// <summary>
        /// Creates a DataMessage out of some input data.
        /// </summary>
        /// <param name="iteration">The iteration number of this message</param>
        /// <param name="data">The data to communicate</param>
        /// <returns>A properly configured DataMessage</returns>
        public abstract DataMessage GetDataMessage<T>(int iteration, params T[] data);

        /// <summary>
        /// Initializes the communication group.
        /// Computation blocks until all required tasks are registered in the group.
        /// </summary>
        /// <param name="cancellationSource">The signal to cancel the operation</param>
        public override void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
            try
            {
                _commLayer.WaitForTaskRegistration(_children.Values.ToList(), cancellationSource, _nodesToRemove);
            }
            catch (Exception e)
            {
                throw new IllegalStateException(
                    "Failed to find parent/children nodes in operator topology for node: " + TaskId, e);
            }

            _initialized = true;

            Send(cancellationSource);
        }

        /// <summary>
        /// Handler for incoming messages from other topology nodes.
        /// </summary>
        /// <param name="message">The message that need to be devlivered to the operator</param>
        public override void OnNext(NsMessage<ElasticGroupCommunicationMessage> message)
        {
            if (_messageQueue.IsAddingCompleted)
            {
                throw new IllegalStateException("Trying to add messages to a closed non-empty queue.");
            }

            _messageQueue.Add(message.Data);

            if (_piggybackTopologyUpdates)
            {
                var topologyPayload = message.Data as DataMessageWithTopology;
                var updates = topologyPayload.TopologyUpdates;

                UpdateTopology(ref updates);
                topologyPayload.TopologyUpdates = updates;
            }

            if (!_children.IsEmpty)
            {
                _sendQueue.Enqueue(message.Data);
            }

            if (_initialized)
            {
                Send(_cancellationSignal);
            }
        }

        /// <summary>
        /// Handler for messages coming from the driver.
        /// </summary>
        /// <param name="message">Message from the driver</param>
        public override void OnNext(DriverMessagePayload message)
        {
            switch (message.PayloadType)
            {
                case DriverMessagePayloadType.Failure:
                    {
                        var rmsg = message as TopologyMessagePayload;

                        foreach (var updates in rmsg.TopologyUpdates)
                        {
                            foreach (var node in updates.Children)
                            {
                                Log.Log(Level.Info, "Removing task {0} from the topology.", node);
                                _nodesToRemove.TryAdd(node, 0);
                                _commLayer.RemoveConnection(node);
                            }
                        }
                        break;
                    }
                case DriverMessagePayloadType.Update:
                    {
                        if (_sendQueue.Count > 0)
                        {
                            if (_sendQueue.TryPeek(out ElasticGroupCommunicationMessage toSendmsg))
                            {
                                var rmsg = message as TopologyMessagePayload;

                                if (_piggybackTopologyUpdates)
                                {
                                    var toSendmsgWithTop = toSendmsg as DataMessageWithTopology;
                                    var updates = rmsg.TopologyUpdates;

                                    UpdateTopology(ref updates);
                                    toSendmsgWithTop.TopologyUpdates = updates;
                                }

                                foreach (var taskId in _nodesToRemove.Keys)
                                {
                                    var id = Utils.GetTaskNum(taskId);
                                    _nodesToRemove.TryRemove(taskId, out byte val);
                                    _children.TryRemove(id, out string str);
                                }
                            }

                            // Unblock this broadcast round.
                            _topologyUpdateReceived.Set();
                        }
                        else
                        {
                            Log.Log(Level.Warning, "Received a topology update message from driver "
                                + "but sending queue is empty: ignoring.");
                        }
                    }
                    break;

                default:
                    throw new ArgumentException(
                        $"Message type {message.PayloadType} not supported by N to one topologies.");
            }
        }

        private void UpdateTopology(ref List<TopologyUpdate> updates)
        {
            var update = updates.Find(elem => elem.Node == TaskId);

            foreach (var child in update.Children)
            {
                if (!_nodesToRemove.TryRemove(child, out byte value))
                {
                    var id = Utils.GetTaskNum(child);
                    _children.TryAdd(id, child);
                }
            }

            if (update != null)
            {
                updates.Remove(update);
            }
        }
    }
}