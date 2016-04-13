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

namespace Org.Apache.REEF.Driver.Task
{
    /// <summary>
    /// DriverTaskState wraps current state and provides methods to move from one state to the next state
    /// </summary>
    sealed public class DriverTaskState
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
            { new TaskStateTransition(TaskTransitionState.TaskRunning, TaskEvent.CompletedTask), TaskTransitionState.TaskCompeleted },
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

        volatile private TaskTransitionState _currentState;

        public DriverTaskState()
        {
            _currentState = TaskTransitionState.TaskNew;
        }

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

        public TaskTransitionState MoveNext(TaskEvent command)
        {
            _currentState = GetNext(command);
            return _currentState;
        }
    }
}
