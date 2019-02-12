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
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Linq;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Default
{
    /// <summary>
    /// Base class for topologies where nodes are allowed to communicated between themselves
    /// and to the driver.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal abstract class OperatorTopologyWithDefaultCommunication :
        DriverAwareOperatorTopology,
        IOperatorTopologyWithCommunication
    {
        protected bool _initialized = false;

        protected DefaultCommunicationLayer _commLayer;

        protected readonly int _disposeTimeout;
        protected readonly int _timeout;
        protected readonly int _retry;

        protected readonly ConcurrentQueue<ElasticGroupCommunicationMessage> _sendQueue = new ConcurrentQueue<ElasticGroupCommunicationMessage>();
        protected readonly BlockingCollection<ElasticGroupCommunicationMessage> _messageQueue = new BlockingCollection<ElasticGroupCommunicationMessage>();
        protected readonly ConcurrentDictionary<int, string> _children = new ConcurrentDictionary<int, string>();
        protected readonly CancellationTokenSource _cancellationSignal = new CancellationTokenSource();

        /// <summary>
        /// Constructor for a communicating topology.
        /// </summary>
        /// <param name="stageName">The stage name the topology is working on</param>
        /// <param name="taskId">The identifier of the task the topology is running on</param>
        /// <param name="rootTaskId">The identifier of the root note in the topology</param>
        /// <param name="operatorId">The identifier of the operator for this topology</param>
        /// <param name="retry">How many times the topology will retry to send a message</param>
        /// <param name="timeout">After how long the topology waits for an event</param>
        /// <param name="disposeTimeout">Maximum wait time for topology disposal</param>
        /// <param name="commLayer">Class responsible for communication</param>
        public OperatorTopologyWithDefaultCommunication(
            string stageName,
            string taskId,
            string rootTaskId,
            int operatorId,
            DefaultCommunicationLayer commLayer,
            int retry,
            int timeout,
            int disposeTimeout) : base(stageName, taskId, rootTaskId, operatorId)
        {
            _commLayer = commLayer;
 
            _retry = retry;
            _timeout = timeout;
            _disposeTimeout = disposeTimeout;
        }

        /// <summary>
        /// Communicate to the driver that the current subscrition has completed its
        /// execution.
        /// </summary>
        public void StageComplete()
        {
            if (TaskId == RootTaskId)
            {
                _commLayer.StageComplete(TaskId, StageName);
            }
        }

        /// <summary>
        /// Request a topology status update to the driver.
        /// </summary>
        public void TopologyUpdateRequest()
        {
            _commLayer.TopologyUpdateRequest(TaskId, StageName, OperatorId);
        }

        /// <summary>
        /// Waiting logic before disposing topologies. 
        /// </summary>
        public override void WaitCompletionBeforeDisposing()
        {
            var tsEnd = DateTime.Now.AddMilliseconds(_disposeTimeout);
            while (_sendQueue.Count > 0 && DateTime.Now < tsEnd)
            {
                // The topology is still trying to send messages, wait.
                Thread.Sleep(100);
                
            }
        }

        /// <summary>
        /// Signal the the current task is joining the topology.
        /// </summary>
        public virtual void JoinTopology()
        {
            _commLayer.JoinTopology(TaskId, StageName, OperatorId);
        }

        /// <summary>
        /// Initializes the communication group.
        /// Computation blocks until all required tasks are registered in the group.
        /// </summary>
        /// <param name="cancellationSource">The signal to cancel the operation</param>
        public virtual void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
            try
            {
                _commLayer.WaitForTaskRegistration(_children.Values.ToList(), cancellationSource);
            }
            catch (Exception e)
            {
                throw new OperationCanceledException("Failed to find parent/children nodes in operator topology for node: " + TaskId, e);
            }

            _initialized = true;

            // Some message may have been received while we were setting up the topology. Send them.
            Send(cancellationSource);
        }

        /// <summary>
        /// Block and wait untill a message is received.
        /// </summary>
        /// <param name="cancellationSource">The signal that the operation is cacelled</param>
        /// <returns></returns>
        public virtual ElasticGroupCommunicationMessage Receive(CancellationTokenSource cancellationSource)
        {
            ElasticGroupCommunicationMessage message;
            int retry = 1;

            while (!_messageQueue.TryTake(out message, _timeout, cancellationSource.Token))
            {
                if (cancellationSource.IsCancellationRequested)
                {
                    throw new OperationCanceledException("Received cancellation request: stop receiving.");
                }

                if (retry++ > _retry)
                {
                    throw new Exception($"Failed to receive message after {_retry} try.");
                }
            }

            return message;
        }

        /// <summary>
        /// Send the input message. This method is asynchornous.
        /// </summary>
        /// <param name="message">The message to communicate</param>
        /// <param name="cancellationSource">The signal for cancelling the operation</param>
        public virtual void Send(ElasticGroupCommunicationMessage message, CancellationTokenSource cancellationSource)
        {
            _sendQueue.Enqueue(message);

            if (_initialized)
            {
                Send(cancellationSource);
            }
        }

        /// <summary>
        /// Handler for incoming messages from other topology nodes.
        /// </summary>
        /// <param name="message">The message that need to be devlivered to the operator</param>
        public virtual void OnNext(NsMessage<ElasticGroupCommunicationMessage> message)
        {
            if (_messageQueue.IsAddingCompleted)
            {
                if (_messageQueue.Count > 0)
                {
                    throw new IllegalStateException("Trying to add messages to a closed non-empty queue.");
                }
            }

            _messageQueue.Add(message.Data);

            // Automatically forward the received message to the child nodes in the topology.
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
        /// Dispose the topology.
        /// </summary>
        public virtual void Dispose()
        {
            _messageQueue.CompleteAdding();

            _cancellationSignal.Cancel();

            _commLayer.Dispose();
        }

        /// <summary>
        /// Logic to execute in case the observable sends an error event.
        /// </summary>
        /// <param name="error">The error throw on the observable.</param>
        public new void OnError(Exception error)
        {
            _messageQueue.CompleteAdding();
        }

        /// <summary>
        /// Logic to execute in case the observable sends a complete event.
        /// </summary>
        /// <param name="error"></param>
        public new void OnCompleted()
        {
            _messageQueue.CompleteAdding();
        }

        /// <summary>
        /// Send a previously queued data message.
        /// </summary>
        /// <param name="cancellationSource">The singal in case the task is cancelled</param>
        protected virtual void Send(CancellationTokenSource cancellationSource)
        {
            ElasticGroupCommunicationMessage message;
            while (_sendQueue.TryDequeue(out message) && !cancellationSource.IsCancellationRequested)
            {
                foreach (var child in _children.Values)
                {
                    _commLayer.Send(child, message, cancellationSource);
                }
            }
        }
    }
}
