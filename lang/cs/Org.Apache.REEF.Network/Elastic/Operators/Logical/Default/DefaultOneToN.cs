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

using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Topology.Logical;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Driver.Task;
using System.Collections.Generic;
using System;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Network.Elastic.Comm.Enum;
using Org.Apache.REEF.Network.Elastic.Failures.Default;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Default
{
    /// <summary>
    /// Generic implementation of an operator having one node sending to N nodes
    /// and with default failure behaviour.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal abstract class DefaultOneToN<T> : ElasticOperatorWithDefaultDispatcher
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultOneToN<>));

        private volatile bool _stop;

        /// <summary>
        /// Constructor for an operator where one node sends to N nodes and with default
        /// failure behavior.
        /// </summary>
        /// <param name="senderId">The identifier of the task sending the message</param>
        /// <param name="prev">The previous node in the pipeline</param>
        /// <param name="topology">The toopology the message routing protocol will use</param>
        /// <param name="failureMachine">The failure machine for this operator</param>
        /// <param name="checkpointLevel">The checkpoint level for the operator</param>
        /// <param name="configurations">Additional operator specific configurations</param>
        public DefaultOneToN(
            int senderId,
            ElasticOperator prev,
            ITopology topology,
            IFailureStateMachine failureMachine,
            CheckpointLevel checkpointLevel,
            params IConfiguration[] configurations) : base(
                null,
                prev,
                topology,
                failureMachine,
                checkpointLevel,
                configurations)
        {
            MasterId = senderId;
            WithinIteration = prev.WithinIteration;

            _stop = false;
        }

        /// <summary>
        /// Operator specific logic for reacting when a task message is received.
        /// </summary>
        /// <param name="message">Incoming message from a task</param>
        /// <param name="returnMessages">Zero or more reply messages for the task</param>
        /// <returns>True if the operator has reacted to the task message</returns>
        protected override bool ReactOnTaskMessage(ITaskMessage message, ref List<IElasticDriverMessage> returnMessages)
        {
            var msgReceived = (TaskMessageType)BitConverter.ToUInt16(message.Message, 0);

            switch (msgReceived)
            {
                case TaskMessageType.JoinTopology:
                    {
                        var operatorId = BitConverter.ToInt16(message.Message, sizeof(ushort));

                        if (operatorId != _id)
                        {
                            return false;
                        }

                        if (!Stage.IsCompleted && _failureMachine.State.FailureState < (int)DefaultFailureStates.Fail)
                        {
                            var taskId = message.TaskId;
                            LOGGER.Log(Level.Info, $"{taskId} joins the topology for operator {_id}");

                            _topology.AddTask(taskId, _failureMachine);
                        }

                        return true;
                    }
                case TaskMessageType.TopologyUpdateRequest:
                    {
                        var operatorId = BitConverter.ToInt16(message.Message, sizeof(ushort));

                        if (operatorId != _id)
                        {
                            return false;
                        }

                        LOGGER.Log(Level.Info, $"Received topology update request for {OperatorName} {_id} from {message.TaskId}");

                        if (!_stop)
                        {
                            _topology.TopologyUpdateResponse(message.TaskId, ref returnMessages, Optional<IFailureStateMachine>.Of(_failureMachine));
                        }
                        else
                        {
                            LOGGER.Log(Level.Info, $"Operator {OperatorName} is in stopped: Ignoring");
                        }

                        return true;
                    }
                case TaskMessageType.CompleteStage:
                    {
                        Stage.IsCompleted = true;

                        return true;
                    }

                default:
                    return false;
            }
        }

        /// <summary>
        /// Mechanism to execute when a reconfigure event is triggered.
        /// <paramref name="reconfigureEvent"/>
        /// </summary>
        public override void OnReconfigure(ref ReconfigureEvent reconfigureEvent)
        {
            LOGGER.Log(Level.Info, $"Going to reconfigure the {OperatorName} operator");

            if (_stop)
            {
                _stop = false;
            }

            if (reconfigureEvent.FailedTask.IsPresent())
            {
                if (reconfigureEvent.FailedTask.Value.AsError() is OperatorException)
                {
                    var info = Optional<string>.Of(((OperatorException)reconfigureEvent.FailedTask.Value.AsError()).AdditionalInfo);
                    var msg = _topology.Reconfigure(reconfigureEvent.FailedTask.Value.Id, info, reconfigureEvent.Iteration);

                    reconfigureEvent.FailureResponse.AddRange(msg);
                }
                else
                {
                    var msg = _topology.Reconfigure(reconfigureEvent.FailedTask.Value.Id, Optional<string>.Empty(), reconfigureEvent.Iteration);

                    reconfigureEvent.FailureResponse.AddRange(msg);
                }
            }
        }

        /// <summary>
        /// Mechanism to execute when a reschedule event is triggered.
        /// <paramref name="rescheduleEvent"/>
        /// </summary>
        public override void OnReschedule(ref RescheduleEvent rescheduleEvent)
        {
            var reconfigureEvent = rescheduleEvent as ReconfigureEvent;

            OnReconfigure(ref reconfigureEvent);
        }

        /// <summary>
        /// Mechanism to execute when a stop event is triggered.
        /// <paramref name="stopEvent"/>
        /// </summary>
        public override void OnStop(ref StopEvent stopEvent)
        {
            if (!_stop)
            {
                _stop = true;
            }
        }
    }
}
