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
using Org.Apache.REEF.IMRU.OnREEF.Exceptions;

namespace Org.Apache.REEF.IMRU.OnREEF.IMRUTasks
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
        private readonly static IDictionary<TaskStateTransition, TaskState> Transitions = new ReadOnlyDictionary<TaskStateTransition, TaskState>(
            new Dictionary<TaskStateTransition, TaskState>
        {
            { new TaskStateTransition(TaskState.TaskNew, TaskEvent.SubmittedTask), TaskState.TaskSubmitted },
            { new TaskStateTransition(TaskState.TaskNew, TaskEvent.ClosedTask), TaskState.TaskClosedByDriver },            
            { new TaskStateTransition(TaskState.TaskSubmitted, TaskEvent.RunningTask), TaskState.TaskRunning },
            { new TaskStateTransition(TaskState.TaskSubmitted, TaskEvent.FailedTaskAppError), TaskState.TaskFailedAppError },
            { new TaskStateTransition(TaskState.TaskSubmitted, TaskEvent.FailedTaskSystemError), TaskState.TaskFailedSystemError },
            { new TaskStateTransition(TaskState.TaskSubmitted, TaskEvent.FailedTaskEvaluatorError), TaskState.TaskFailedByEvaluatorFailure },
            { new TaskStateTransition(TaskState.TaskSubmitted, TaskEvent.FailedTaskCommunicationError), TaskState.TaskFailedByGroupCommunication },
            { new TaskStateTransition(TaskState.TaskRunning, TaskEvent.CompletedTask), TaskState.TaskCompleted },
            { new TaskStateTransition(TaskState.TaskRunning, TaskEvent.WaitingTaskToClose), TaskState.TaskWaitingForClose },
            { new TaskStateTransition(TaskState.TaskRunning, TaskEvent.FailedTaskAppError), TaskState.TaskFailedAppError },
            { new TaskStateTransition(TaskState.TaskRunning, TaskEvent.FailedTaskSystemError), TaskState.TaskFailedSystemError },
            { new TaskStateTransition(TaskState.TaskRunning, TaskEvent.FailedTaskEvaluatorError), TaskState.TaskFailedByEvaluatorFailure },
            { new TaskStateTransition(TaskState.TaskRunning, TaskEvent.FailedTaskCommunicationError), TaskState.TaskFailedByGroupCommunication },
            { new TaskStateTransition(TaskState.TaskWaitingForClose, TaskEvent.ClosedTask), TaskState.TaskClosedByDriver },
            { new TaskStateTransition(TaskState.TaskFailedSystemError, TaskEvent.FailedTaskEvaluatorError), TaskState.TaskFailedByEvaluatorFailure },
            { new TaskStateTransition(TaskState.TaskFailedByGroupCommunication, TaskEvent.FailedTaskEvaluatorError), TaskState.TaskFailedByEvaluatorFailure }
        });

        private readonly static ISet<TaskState> FinalState = new HashSet<TaskState>()
        {
            TaskState.TaskFailedAppError,
            TaskState.TaskFailedSystemError,
            TaskState.TaskFailedByEvaluatorFailure,
            TaskState.TaskFailedByGroupCommunication,
            TaskState.TaskClosedByDriver,
            TaskState.TaskCompleted
        };

        private volatile TaskState _currentState;
        private readonly object _lock = new object();

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
        /// If there is no valid next state, ApplicationException will be thrown.
        /// </summary>
        /// <param name="taskEvent"></param>
        /// <returns></returns>
        internal TaskState GetNext(TaskEvent taskEvent)
        {
            TaskStateTransition transition = new TaskStateTransition(_currentState, taskEvent);
            TaskState nextState;
            if (!Transitions.TryGetValue(transition, out nextState))
            {
                throw new TaskStateTransitionException(_currentState, taskEvent);
            }
            return nextState;
        }

        /// <summary>
        /// Move to the next state
        /// If it is not able to move to the next valid state for a given event, ApplicationException will be thrown.
        /// </summary>
        /// <param name="taskEvent"></param>
        /// <returns></returns>
        internal TaskState MoveNext(TaskEvent taskEvent)
        {
            lock (_lock)
            {
                _currentState = GetNext(taskEvent);
                return _currentState;
            }
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
