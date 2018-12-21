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

using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Tang.Annotations;
using System.Collections.Generic;
using Org.Apache.REEF.Common.Tasks;
using System.Threading;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Utilities.Logging;
using System.Linq;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    /// <summary>
    /// Topology class managing data communication for broadcast operators.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal sealed class DefaultBroadcastTopology : OneToNTopology
    {
        [Inject]
        private DefaultBroadcastTopology(
            [Parameter(typeof(OperatorParameters.SubscriptionName))] string subscriptionName,
            [Parameter(typeof(OperatorParameters.TopologyRootTaskId))] int rootId,
            [Parameter(typeof(OperatorParameters.TopologyChildTaskIds))] ISet<int> children,
            [Parameter(typeof(OperatorParameters.PiggybackTopologyUpdates))] bool piggyback,
            [Parameter(typeof(OperatorParameters.OperatorId))] int operatorId,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Retry))] int retry,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.Timeout))] int timeout,
            [Parameter(typeof(GroupCommunicationConfigurationOptions.DisposeTimeout))] int disposeTimeout,
            CommunicationService commLayer,
            CheckpointService checkpointService) : base(
                subscriptionName,
                taskId,
                Utils.BuildTaskId(subscriptionName, rootId),
                operatorId,
                children,
                piggyback,
                retry,
                timeout,
                disposeTimeout,
                commLayer,
                checkpointService)
        {
        }

        public override DataMessage AssembleDataMessage<T>(int iteration, T[] data)
        {
            if (_piggybackTopologyUpdates)
            {
                return new DataMessageWithTopology<T>(SubscriptionName, OperatorId, iteration, data[0]);
            }
            else
            {
                return new DataMessage<T>(SubscriptionName, OperatorId, iteration, data[0]);
            }
        }

        /// <summary>
        /// Send a previously queued data message.
        /// </summary>
        /// <param name="cancellationSource">The source in case the task is cancelled</param>
        protected override void Send(CancellationTokenSource cancellationSource)
        {
            GroupCommunicationMessage message;
            int retry = 0;

            // Check if we have a message to send
            if (_sendQueue.TryPeek(out message))
            {
                var dm = message as DataMessage;

                // Broadcast topology require the driver to send topology updates to the root node
                // in order to have the most update topology at each boradcast round.
                while (!_topologyUpdateReceived.WaitOne(_timeout))
                {
                    // If we are here, we weren't able to receive a topology update on time. Retry.
                    if (cancellationSource.IsCancellationRequested)
                    {
                        LOGGER.Log(Level.Warning, "Received cancellation request: stop sending");
                        return;
                    }

                    retry++;

                    if (retry > _retry)
                    {
                        throw new OperatorException($"Iteration {dm.Iteration}: Failed to send message to the next node in the ring after {_retry} try.", OperatorId);
                    }

                    TopologyUpdateRequest();
                }

                // Get the actual message to send. Note that altough message sending is asynchronous, broadcast rounds should not overlap.
                _sendQueue.TryDequeue(out message);

                if (TaskId == RootTaskId)
                {
                    // Prepare the mutex to block for the next round of topology updates.
                    _topologyUpdateReceived.Reset();
                }

                // Deliver the message to the commonication layer.
                foreach (var node in _children.Where(x => !_nodesToRemove.TryGetValue(x.Value, out byte val)))
                {
                    _commService.Send(node.Value, message, cancellationSource);
                }
            }
        }
    }
}
