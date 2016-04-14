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

namespace Org.Apache.REEF.Driver.Task
{
    /// <summary>
    /// DriverTaskState wraps current state and provides methods to move from one state to the next state
    /// </summary>
    public sealed class DriverTaskState
    {
        internal static IDictionary<TaskStateTransition, TaskTransitionState> Transitions = new Dictionary<TaskStateTransition, TaskTransitionState>
        {
            { new TaskStateTransition(TaskTransitionState.TaskNew, TaskEvent.SubmittedTask), TaskTransitionState.TaskSubmitting },
            { new TaskStateTransition(TaskTransitionState.TaskNew, TaskEvent.ClosedTask), TaskTransitionState.TaskClosedByDriver },            
            { new TaskStateTransition(TaskTransitionState.TaskSubmitting, TaskEvent.RunningTask), TaskTransitionState.TaskRunning },
            { new TaskStateTransition(TaskTransitionState.TaskSubmitting, TaskEvent.FailedTaskAppError), TaskTransitionState.TaskFailedAppError },
            { new TaskStateTransition(TaskTransitionState.TaskSubmitting, TaskEvent.FailedTaskSystemError), TaskTransitionState.TaskFailedSystemError },
            { new TaskStateTransition(TaskTransitionState.TaskSubmitting, TaskEvent.FailedTaskEvaluatorError), TaskTransitionState.TaskFailedByEvaluatorFailure },
            { new TaskStateTransition(TaskTransitionState.TaskSubmitting, TaskEvent.FailedTaskCommunicationError), TaskTransitionState.TaskFailedByGroupCommunication },
            { new TaskStateTransition(TaskTransitionState.TaskRunning, TaskEvent.CompletedTask), TaskTransitionState.TaskCompleted },
            { new TaskStateTransition(TaskTransitionState.TaskRunning, TaskEvent.WaitingTaskToClose), TaskTransitionState.TaskWaitingForClose },
            { new TaskStateTransition(TaskTransitionState.TaskRunning, TaskEvent.FailedTaskAppError), TaskTransitionState.TaskFailedAppError },
            { new TaskStateTransition(TaskTransitionState.TaskRunning, TaskEvent.FailedTaskSystemError), TaskTransitionState.TaskFailedSystemError },
            { new TaskStateTransition(TaskTransitionState.TaskRunning, TaskEvent.FailedTaskEvaluatorError), TaskTransitionState.TaskFailedByEvaluatorFailure },
            { new TaskStateTransition(TaskTransitionState.TaskRunning, TaskEvent.FailedTaskCommunicationError), TaskTransitionState.TaskFailedByGroupCommunication },
            { new TaskStateTransition(TaskTransitionState.TaskWaitingForClose, TaskEvent.ClosedTask), TaskTransitionState.TaskClosedByDriver },
            { new TaskStateTransition(TaskTransitionState.TaskWaitingForClose, TaskEvent.FailedTaskAppError), TaskTransitionState.TaskFailedAppError },
            { new TaskStateTransition(TaskTransitionState.TaskWaitingForClose, TaskEvent.FailedTaskSystemError), TaskTransitionState.TaskFailedSystemError },
            { new TaskStateTransition(TaskTransitionState.TaskWaitingForClose, TaskEvent.FailedTaskEvaluatorError), TaskTransitionState.TaskFailedByEvaluatorFailure },
            { new TaskStateTransition(TaskTransitionState.TaskWaitingForClose, TaskEvent.FailedTaskCommunicationError), TaskTransitionState.TaskFailedByGroupCommunication },
            { new TaskStateTransition(TaskTransitionState.TaskFailedSystemError, TaskEvent.FailedTaskEvaluatorError), TaskTransitionState.TaskFailedByEvaluatorFailure },
            { new TaskStateTransition(TaskTransitionState.TaskFailedByGroupCommunication, TaskEvent.FailedTaskEvaluatorError), TaskTransitionState.TaskFailedByEvaluatorFailure }
        };

        private static TaskTransitionState[] FinalStatae = 
        {
            TaskTransitionState.TaskFailedAppError,
            TaskTransitionState.TaskFailedSystemError,
            TaskTransitionState.TaskFailedByEvaluatorFailure,
            TaskTransitionState.TaskFailedByGroupCommunication,
            TaskTransitionState.TaskClosedByDriver,
            TaskTransitionState.TaskCompleted
        };

        private volatile TaskTransitionState _currentState;
        private readonly object _lock = new object();

        /// <summary>
        /// Create a new DriverTaskState with TaskNew as the task initial state
        /// </summary>
        public DriverTaskState()
        {
            _currentState = TaskTransitionState.TaskNew;
        }

        /// <summary>
        /// return the current task state
        /// </summary>
        public TaskTransitionState CurrentState
        {
            get
            {
                return _currentState;
            }
        }

        private TaskTransitionState GetNext(TaskEvent taskEvent)
        {
            TaskStateTransition transition = new TaskStateTransition(_currentState, taskEvent);
            TaskTransitionState nextState;
            if (!Transitions.TryGetValue(transition, out nextState))
            {
                throw new ApplicationException("Invalid transition: " + _currentState + " -> " + taskEvent);
            }
            return nextState;
        }

        /// <summary>
        /// Move to the next state
        /// </summary>
        /// <param name="command"></param>
        /// <returns></returns>
        public TaskTransitionState MoveNext(TaskEvent command)
        {
            lock (_lock)
            {
                _currentState = GetNext(command);
                return _currentState;
            }
        }

        /// <summary>
        /// Check it the task has reached to a final state
        /// </summary>
        /// <returns></returns>
        public bool IsFinalState()
        {
            return FinalStatae.Contains(_currentState);
        }

        /// <summary>
        /// Checks if the given TaskTransitionState is a final state
        /// </summary>
        /// <param name="taskState"></param>
        /// <returns></returns>
        public static bool IsFinalState(TaskTransitionState taskState)
        {
            return FinalStatae.Contains(taskState);
        }
    }
}
