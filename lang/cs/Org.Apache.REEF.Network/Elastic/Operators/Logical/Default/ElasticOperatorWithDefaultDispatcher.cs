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

using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Elastic.Topology.Logical;
using System.Collections.Generic;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Wake.Time.Event;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Network.Elastic.Failures.Default;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Default
{
    /// <summary>
    /// Abstract operator implementing the default failure logic.
    /// This can be used as super class for default operators.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal abstract class ElasticOperatorWithDefaultDispatcher : ElasticOperator, IDefaultFailureEventResponse
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(ElasticOperatorWithDefaultDispatcher));

        /// <summary>
        /// Base constructor for an abstract operator implementing the default failure logic.
        /// </summary>
        /// <param name="stage">The stage the operator is part of</param>
        /// <param name="prev">The previous operator in the pipelines</param>
        /// <param name="topology">The topology for the operator</param>
        /// <param name="failureMachine">The failure machine of the operator</param>
        /// <param name="level">The chckpoint level for the opearator</param>
        /// <param name="configurations">Additonal opeartor specific configurations</param>
        protected ElasticOperatorWithDefaultDispatcher(
            IElasticStage stage,
            ElasticOperator prev, ITopology topology,
            IFailureStateMachine failureMachine,
            CheckpointLevel checkpointLevel = CheckpointLevel.None,
            params IConfiguration[] configurations) :
        base(stage, prev, topology, failureMachine, checkpointLevel, configurations)
        {
        }

        /// <summary>
        /// Add the broadcast operator to the operator pipeline.
        /// </summary>
        /// <typeparam name="T">The type of messages that the operator will send / receive</typeparam>
        /// <param name="senderId">The id of the sender / root node of the broadcast</param>
        /// <param name="topology">The topology of the operator</param>
        /// <param name="failureMachine">The failure state machine of the operator</param>
        /// <param name="checkpointLevel">The checkpoint policy for the operator</param>
        /// <param name="configurations">Additional configurations for the operator</param>
        /// <returns>The same operator pipeline with the added broadcast operator</returns>
        public override ElasticOperator Broadcast<T>(
            int senderId,
            ITopology topology,
            IFailureStateMachine failureMachine,
            CheckpointLevel checkpointLevel,
            params IConfiguration[] configurations)
        {
            _next = new DefaultBroadcast<T>(senderId, this, topology, failureMachine, checkpointLevel, configurations);
            return _next;
        }

        /// <summary>
        /// Used to react on a failure occurred on a task.
        /// It gets a failed task as input and in response it produces zero or more failure events.
        /// </summary>
        /// <param name="task">The failed task</param>
        /// <param name="failureEvents">A list of events encoding the type of actions to be triggered so far</param>
        /// <exception cref="Exception">If the task failure cannot be properly handled</exception>
        public override void OnTaskFailure(IFailedTask task, ref List<IFailureEvent> failureEvents)
        {
            var failedOperatorId = (task.AsError() as OperatorException)?.OperatorId ?? _id;

            if (WithinIteration || failedOperatorId <= _id)
            {
                int lostDataPoints = _topology.RemoveTask(task.Id);
                var failureState = _failureMachine.RemoveDataPoints(lostDataPoints);

                switch ((DefaultFailureStates)failureState.FailureState)
                {
                    case DefaultFailureStates.ContinueAndReconfigure:
                        failureEvents.Add(new ReconfigureEvent(task, _id));
                        break;

                    case DefaultFailureStates.ContinueAndReschedule:
                        if (failedOperatorId == _id)
                        {
                            var @event = new Failures.Default.RescheduleEvent(task.Id)
                            {
                                FailedTask = Optional<IFailedTask>.Of(task)
                            };
                            failureEvents.Add(@event);
                        }
                        break;

                    case DefaultFailureStates.StopAndReschedule:
                        {
                            var @event = new StopEvent(task.Id);
                            if (failedOperatorId == _id)
                            {
                                @event.FailedTask = Optional<IFailedTask>.Of(task);
                            }
                            failureEvents.Add(@event);
                        }
                        break;

                    case DefaultFailureStates.Fail:
                        failureEvents.Add(new FailEvent(task.Id));
                        break;

                    default:
                        Log.Log(Level.Info, "Failure from {0} requires no action", task.Id);
                        break;
                }

                LogOperatorState();
            }

            if (PropagateFailureDownstream())
            {
                _next?.OnTaskFailure(task, ref failureEvents);
            }
        }

        /// <summary>
        /// Used to react when a timeout event is triggered.
        /// </summary>
        /// <param name="alarm">The alarm triggering the timeput</param>
        /// <param name="msgs">A list of messages encoding how remote tasks need to react</param>
        /// <param name="nextTimeouts">The next timeouts to be scheduled</param>
        public override void OnTimeout(
            Alarm alarm,
            ref List<IElasticDriverMessage> msgs,
            ref List<ITimeout> nextTimeouts)
        {
            _next?.OnTimeout(alarm, ref msgs, ref nextTimeouts);
        }

        /// <summary>
        /// When a new failure state is reached, this method is used to dispatch
        /// such event to the proper failure mitigation logic.
        /// It gets a failure event as input and produces zero or more failure response messages
        /// for tasks (appended into the event).
        /// </summary>
        /// <param name="event">The failure event to react upon</param>
        public override void EventDispatcher(ref IFailureEvent @event)
        {
            if (@event.OperatorId == _id || @event.OperatorId < 0)
            {
                switch ((DefaultFailureStateEvents)@event.FailureEvent)
                {
                    case DefaultFailureStateEvents.Reconfigure:
                        var rec = @event as ReconfigureEvent;
                        OnReconfigure(ref rec);
                        break;

                    case DefaultFailureStateEvents.Reschedule:
                        var res = @event as RescheduleEvent;
                        OnReschedule(ref res);
                        break;

                    case DefaultFailureStateEvents.Stop:
                        var stp = @event as StopEvent;
                        OnStop(ref stp);
                        break;

                    default:
                        OnFail();
                        break;
                }
            }

            if (@event.OperatorId == -1 || @event.OperatorId > _id)
            {
                _next?.EventDispatcher(ref @event);
            }
        }

        /// <summary>
        /// Mechanism to execute when a reconfigure event is triggered.
        /// <paramref name="reconfigureEvent"/>
        /// </summary>
        public virtual void OnReconfigure(ref ReconfigureEvent reconfigureEvent)
        {
        }

        /// <summary>
        /// Mechanism to execute when a reschedule event is triggered.
        /// <paramref name="rescheduleEvent"/>
        /// </summary>
        public virtual void OnReschedule(ref RescheduleEvent rescheduleEvent)
        {
        }

        /// <summary>
        /// Mechanism to execute when a stop event is triggered.
        /// <paramref name="stopEvent"/>
        /// </summary>
        public virtual void OnStop(ref StopEvent stopEvent)
        {
        }

        /// <summary>
        /// Mechanism to execute when a fail event is triggered.
        /// </summary>
        public virtual void OnFail()
        {
        }

        /// <summary>
        /// Returns whether a failure should be propagated to downstream operators or not.
        /// </summary>
        /// <returns>True if the failure has to be sent downstream</returns>
        protected override bool PropagateFailureDownstream()
        {
            return _failureMachine.State.FailureState.IsContinue() || 
                _failureMachine.State.FailureState.IsContinueAndReconfigure() ||
                _failureMachine.State.FailureState.IsContinueAndReschedule();
        }

        /// <summary>
        /// Logs the current operator state.
        /// </summary>
        protected override void LogOperatorState()
        {
            if (Log.IsLoggable(Level.Info))
            {
                Log.Log(Level.Info,
                    "State for Operator {0} in Stage {1}:\n" +
                    "Topology:\n{2}\n" +
                    "Failure State: {3}\n" +
                    "Failure(s) Reported: {4}/{5}",
                    OperatorType, Stage.StageName, _topology.LogTopologyState(),
                     (DefaultFailureStates)_failureMachine.State.FailureState,
                    _failureMachine.NumOfFailedDataPoints, _failureMachine.NumOfDataPoints);
            }
        }
    }
}