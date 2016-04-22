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
using System.Collections.ObjectModel;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver.StateMachine
{
    /// <summary>
    /// This is a state transition machine which wraps current state, defines valid state transitions and API to move to next state. 
    /// All the system states are defined in <see cref="SystemState"></see>
    /// For the system state transition diagram <see href="https://issues.apache.org/jira/browse/REEF-1335"></see>
    /// </summary>
    internal sealed class SystemStateMachine
    {
        internal readonly static IDictionary<StateTransition<SystemState, SystemStateEvent>, SystemState> Transitions = 
            new ReadOnlyDictionary<StateTransition<SystemState, SystemStateEvent>, SystemState>(
                new Dictionary<StateTransition<SystemState, SystemStateEvent>, SystemState>()
        {
            { new StateTransition<SystemState, SystemStateEvent>(SystemState.WaitingForEvaluator, SystemStateEvent.AllContextsAreReady), SystemState.SubmittingTasks },
            { new StateTransition<SystemState, SystemStateEvent>(SystemState.WaitingForEvaluator, SystemStateEvent.NotRecoverable), SystemState.Fail },
            { new StateTransition<SystemState, SystemStateEvent>(SystemState.WaitingForEvaluator, SystemStateEvent.FailedNode), SystemState.WaitingForEvaluator },
            { new StateTransition<SystemState, SystemStateEvent>(SystemState.SubmittingTasks, SystemStateEvent.AllTasksAreRunning), SystemState.TasksRunning },
            { new StateTransition<SystemState, SystemStateEvent>(SystemState.SubmittingTasks, SystemStateEvent.FailedNode), SystemState.ShuttingDown },
            { new StateTransition<SystemState, SystemStateEvent>(SystemState.TasksRunning, SystemStateEvent.FailedNode), SystemState.ShuttingDown },
            { new StateTransition<SystemState, SystemStateEvent>(SystemState.TasksRunning, SystemStateEvent.AllTasksAreCompleted), SystemState.TasksCompleted },
            { new StateTransition<SystemState, SystemStateEvent>(SystemState.ShuttingDown, SystemStateEvent.FailedNode), SystemState.ShuttingDown },
            { new StateTransition<SystemState, SystemStateEvent>(SystemState.ShuttingDown, SystemStateEvent.Recover), SystemState.WaitingForEvaluator },
            { new StateTransition<SystemState, SystemStateEvent>(SystemState.ShuttingDown, SystemStateEvent.NotRecoverable), SystemState.Fail }
        });

        private SystemState _currentState;

        /// <summary>
        /// create a new SysteState starting from WaitingForEvaluator
        /// </summary>
        internal SystemStateMachine()
        {
            _currentState = SystemState.WaitingForEvaluator;
        }

        /// <summary>
        /// Returns current state
        /// </summary>
        internal SystemState CurrentState
        {
            get { return _currentState; }
        }

        /// <summary>
        /// Based on the event and current state to determine the next state
        /// </summary>
        /// <param name="systemEvent"></param>
        /// <returns></returns>
        internal SystemState GetNext(SystemStateEvent systemEvent)
        {
            var transition = new StateTransition<SystemState, SystemStateEvent>(_currentState, systemEvent);
            SystemState nextState;
            if (!Transitions.TryGetValue(transition, out nextState))
            {
                throw new SystemStateTransitionException(_currentState, systemEvent);
            }
            return nextState;
        }

        /// <summary>
        /// Move to next state based on the event given
        /// </summary>
        /// <param name="stateEvent"></param>
        /// <returns></returns>
        internal SystemState MoveNext(SystemStateEvent stateEvent)
        {
            _currentState = GetNext(stateEvent);
            return _currentState;
        }
    }
}
