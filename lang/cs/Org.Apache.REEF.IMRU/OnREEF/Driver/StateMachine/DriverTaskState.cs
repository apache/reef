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
    /// Driver Task State represents task states from creating a new task, to submitted task, to task running, until task is completed. 
    /// It also defines the state transition condition from one to another
    /// All the task states are defined in <see cref="TaskState"></see>
    /// For the task state transition diagram <see href="https://issues.apache.org/jira/browse/REEF-1327"></see>
    /// This class wraps current state and provides methods to move from one state to the next state
    /// </summary>
    internal sealed class DriverTaskState
    {
        private readonly static IDictionary<StateTransition<TaskState, TaskStateEvent>, TaskState> Transitions = new ReadOnlyDictionary<StateTransition<TaskState, TaskStateEvent>, TaskState>(
            new Dictionary<StateTransition<TaskState, TaskStateEvent>, TaskState>
        {
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskNew, TaskStateEvent.SubmittedTask), TaskState.TaskSubmitted },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskNew, TaskStateEvent.ClosedTask), TaskState.TaskClosedByDriver },            
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskSubmitted, TaskStateEvent.RunningTask), TaskState.TaskRunning },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskSubmitted, TaskStateEvent.FailedTaskAppError), TaskState.TaskFailedByAppError },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskSubmitted, TaskStateEvent.FailedTaskSystemError), TaskState.TaskFailedBySystemError },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskSubmitted, TaskStateEvent.FailedTaskEvaluatorError), TaskState.TaskFailedByEvaluatorFailure },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskSubmitted, TaskStateEvent.FailedTaskCommunicationError), TaskState.TaskFailedByGroupCommunication },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskRunning, TaskStateEvent.CompletedTask), TaskState.TaskCompleted },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskRunning, TaskStateEvent.WaitingTaskToClose), TaskState.TaskWaitingForClose },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskRunning, TaskStateEvent.FailedTaskAppError), TaskState.TaskFailedByAppError },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskRunning, TaskStateEvent.FailedTaskSystemError), TaskState.TaskFailedBySystemError },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskRunning, TaskStateEvent.FailedTaskEvaluatorError), TaskState.TaskFailedByEvaluatorFailure },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskRunning, TaskStateEvent.FailedTaskCommunicationError), TaskState.TaskFailedByGroupCommunication },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskWaitingForClose, TaskStateEvent.ClosedTask), TaskState.TaskClosedByDriver },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskWaitingForClose, TaskStateEvent.FailedTaskAppError), TaskState.TaskClosedByDriver },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskWaitingForClose, TaskStateEvent.FailedTaskSystemError), TaskState.TaskClosedByDriver },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskWaitingForClose, TaskStateEvent.FailedTaskEvaluatorError), TaskState.TaskClosedByDriver },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskWaitingForClose, TaskStateEvent.FailedTaskCommunicationError), TaskState.TaskClosedByDriver },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskFailedBySystemError, TaskStateEvent.FailedTaskEvaluatorError), TaskState.TaskFailedByEvaluatorFailure },
            { new StateTransition<TaskState, TaskStateEvent>(TaskState.TaskFailedByGroupCommunication, TaskStateEvent.FailedTaskEvaluatorError), TaskState.TaskFailedByEvaluatorFailure }
        });

        private readonly static ISet<TaskState> FinalState = new HashSet<TaskState>()
        {
            TaskState.TaskFailedByAppError,
            TaskState.TaskFailedBySystemError,
            TaskState.TaskFailedByEvaluatorFailure,
            TaskState.TaskFailedByGroupCommunication,
            TaskState.TaskClosedByDriver,
            TaskState.TaskCompleted
        };

        private volatile TaskState _currentState;

        /// <summary>
        /// Create a new DriverTaskState with TaskNew as the task initial state
        /// </summary>
        internal DriverTaskState()
        {
            _currentState = TaskState.TaskNew;
        }

        /// <summary>
        /// return the current task state
        /// </summary>
        internal TaskState CurrentState
        {
            get
            {
                return _currentState;
            }
        }

        /// <summary>
        /// Get next valid state based on the current state and event given without changing the current state
        /// If there is no valid next state, TaskStateTransitionException will be thrown.
        /// </summary>
        /// <param name="taskEvent"></param>
        /// <returns></returns>
        internal TaskState GetNext(TaskStateEvent taskEvent)
        {
            StateTransition<TaskState, TaskStateEvent> transition = new StateTransition<TaskState, TaskStateEvent>(_currentState, taskEvent);
            TaskState nextState;
            if (!Transitions.TryGetValue(transition, out nextState))
            {
                throw new TaskStateTransitionException(_currentState, taskEvent);
            }
            return nextState;
        }

        /// <summary>
        /// Move to the next state
        /// If it is not able to move to the next valid state for a given event, TaskStateTransitionException will be thrown.
        /// </summary>
        /// <param name="taskEvent"></param>
        /// <returns></returns>
        internal TaskState MoveNext(TaskStateEvent taskEvent)
        {
            _currentState = GetNext(taskEvent);
            return _currentState;
        }

        /// <summary>
        /// Checks if the current state is a final state
        /// </summary>
        /// <returns></returns>
        internal bool IsFinalState()
        {
            return FinalState.Contains(_currentState);
        }

        /// <summary>
        /// Checks if the given TaskState is a final state
        /// </summary>
        /// <param name="taskState"></param>
        /// <returns></returns>
        internal static bool IsFinalState(TaskState taskState)
        {
            return FinalState.Contains(taskState);
        }
    }
}
