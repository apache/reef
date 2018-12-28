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

using System.Threading;
using System.Collections.Generic;
using Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl;
using Org.Apache.REEF.Network.Elastic.Failures;
using System;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Network.Elastic.Operators.Physical.Enum;
using Org.Apache.REEF.Network.Elastic.Comm;

namespace Org.Apache.REEF.Network.Elastic.Operators.Physical.Impl
{
    /// <summary>
    /// Generic implementation of a group communication operator where one node sends to N.
    /// </summary>
    /// <typeparam name="T">The type of message being sent.</typeparam>
    [Unstable("0.16", "API may change")]
    public abstract class DefaultOneToN<T>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultOneToN<>));

        private readonly ICheckpointableState _checkpointableState;
        internal readonly OneToNTopology _topology;
        internal volatile PositionTracker _position;

        private readonly bool _isLast;

        /// <summary>
        /// Creates a new one to N operator.
        /// </summary>
        /// <param name="id">The operator identifier</param>
        /// <param name="level">The checkpoint level for the operator</param>
        /// <param name="isLast">Whether this operator is the last in the pipeline</param>
        /// <param name="topology">The operator topology layer</param>
        internal DefaultOneToN(int id, bool isLast, ICheckpointableState checkpointableState, OneToNTopology topology)
        {
            OperatorId = id;
            _checkpointableState = checkpointableState;
            _isLast = isLast;
            _topology = topology;
            _position = PositionTracker.Nil;

            OnTaskRescheduled = new Action(() =>
            {
                _topology.JoinTopology();
            });
        }

        /// <summary>
        /// The operator identifier.
        /// </summary>
        public int OperatorId { get; private set; }

        /// <summary>
        /// The operator name.
        /// </summary>
        public string OperatorName { get; protected set; }

        /// <summary>
        /// Operator-specific information that is sent to the driver in case of failure.
        /// </summary>
        public string FailureInfo
        {
            get
            {
                string iteration = IteratorReference == null ? "-1" : IteratorReference.Current.ToString();
                string position = ((int)_position).ToString() + ":";
                string isSending = _topology.IsSending ? "1" : "0";
                return iteration + ":" + position + ":" + isSending;
            }
        }

        /// <summary>
        /// Get a reference of the iterator in the pipeline (if it exists).
        /// </summary>
        public IElasticIterator IteratorReference { protected get; set; }

        /// <summary>
        /// Cancellation source for stopping the exeuction of the opearator.
        /// </summary>
        public CancellationTokenSource CancellationSource { get; set; }

        /// <summary>
        /// Action to execute when a task is re-scheduled.
        /// </summary>
        public Action OnTaskRescheduled { get; private set; }

        /// <summary>
        /// The set of messages checkpointed in memory.
        /// </summary>
        private List<ElasticGroupCommunicationMessage> CheckpointedMessages { get; set; }

        /// <summary>
        /// Receive a message from neighbors broadcasters.
        /// </summary>
        /// <returns>The incoming data</returns>
        public T Receive()
        {
            _position = PositionTracker.InReceive;

            var received = false;
            DataMessage dataMessage = null;
            ITypedDataMessage<T> typedDataMessage = null;
            var isIterative = IteratorReference != null;

            while (!received && !CancellationSource.IsCancellationRequested)
            {
                dataMessage = _topology.Receive(CancellationSource) as DataMessage;
                typedDataMessage = dataMessage as ITypedDataMessage<T>;

                if (isIterative && typedDataMessage.Iteration < (int)IteratorReference.Current)
                {
                    LOGGER.Log(Level.Warning, $"Received message for iteration {typedDataMessage.Iteration} but I am already in iteration {(int)IteratorReference.Current}: ignoring.");
                }
                else
                {
                    received = true;
                }
            }

            if (typedDataMessage == null)
            {
                throw new OperationCanceledException("Impossible to receive messages: operation cancelled.");
            }

            if (isIterative)
            {
                IteratorReference.SyncIteration(typedDataMessage.Iteration);
            }

            Checkpoint(dataMessage, dataMessage.Iteration);

            _position = PositionTracker.AfterReceive;

            return typedDataMessage.Data;
        }

        /// <summary>
        /// Reset the internal position tracker. This should be called
        /// every time a new iteration start in the workflow.
        /// </summary>
        public void ResetPosition()
        {
            _position = PositionTracker.Nil;
        }

        /// <summary>
        /// Initializes the communication group.
        /// Computation blocks until all required tasks are registered in the group.
        /// </summary>
        /// <param name="cancellationSource"></param>
        public void WaitForTaskRegistration(CancellationTokenSource cancellationSource)
        {
            LOGGER.Log(Level.Info, $"Waiting for task registration for {OperatorName} operator.");
            _topology.WaitForTaskRegistration(cancellationSource);
        }

        /// <summary>
        /// Wait until computation is globally completed for this operator 
        /// before disposing the object.
        /// </summary>
        public void WaitCompletionBeforeDisposing()
        {
            _topology.WaitCompletionBeforeDisposing(CancellationSource);
        }

        /// <summary>
        /// Dispose the operator.
        /// </summary>
        public void Dispose()
        {
            if (_isLast)
            {
                _topology.StageComplete();
            }
            _topology.Dispose();
        }

        /// <summary>
        /// Checkpoint the input data for the input iteration using the defined checkpoint level.
        /// </summary>
        /// <param name="data">The messages to checkpoint</param>
        /// <param name="iteration">The iteration of the checkpoint</param>
        internal void Checkpoint(ElasticGroupCommunicationMessage data, int iteration)
        {
            if (_checkpointableState.Level > CheckpointLevel.None)
            {
                var state = _checkpointableState.Create();

                state.MakeCheckpointable(data);
                _topology.Checkpoint(state, iteration);
            }
        }
    }
}
