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
    sealed public class DriverTaskState
    {
        internal static IDictionary<TaskStateTransition, TaskState> Transitions = new Dictionary<TaskStateTransition, TaskState>
        {
            { new TaskStateTransition(TaskState.TaskNew, TaskEvent.SubmittedTask), TaskState.TaskSubmitting },
            { new TaskStateTransition(TaskState.TaskNew, TaskEvent.ClosedTask), TaskState.TaskClosedByDriver },            
            { new TaskStateTransition(TaskState.TaskSubmitting, TaskEvent.RunningTask), TaskState.TaskRunning },
            { new TaskStateTransition(TaskState.TaskSubmitting, TaskEvent.FailedTaskAppError), TaskState.TaskFailedAppError },
            { new TaskStateTransition(TaskState.TaskSubmitting, TaskEvent.FailedTaskSystemError), TaskState.TaskFailedSystemError },
            { new TaskStateTransition(TaskState.TaskSubmitting, TaskEvent.FailedTaskEvaluatorError), TaskState.TaskFailedByEvaluatorFailure },
            { new TaskStateTransition(TaskState.TaskSubmitting, TaskEvent.FailedTaskCommunicationError), TaskState.TaskFailedByGroupCommunication },
            { new TaskStateTransition(TaskState.TaskRunning, TaskEvent.CompletedTask), TaskState.TaskCompeleted },
            { new TaskStateTransition(TaskState.TaskRunning, TaskEvent.WaitingTaskToClose), TaskState.TaskWaitingForClose },
            { new TaskStateTransition(TaskState.TaskRunning, TaskEvent.FailedTaskAppError), TaskState.TaskFailedAppError },
            { new TaskStateTransition(TaskState.TaskRunning, TaskEvent.FailedTaskSystemError), TaskState.TaskFailedSystemError },
            { new TaskStateTransition(TaskState.TaskRunning, TaskEvent.FailedTaskEvaluatorError), TaskState.TaskFailedByEvaluatorFailure },
            { new TaskStateTransition(TaskState.TaskRunning, TaskEvent.FailedTaskCommunicationError), TaskState.TaskFailedByGroupCommunication },
            { new TaskStateTransition(TaskState.TaskWaitingForClose, TaskEvent.ClosedTask), TaskState.TaskClosedByDriver },
            { new TaskStateTransition(TaskState.TaskWaitingForClose, TaskEvent.FailedTaskAppError), TaskState.TaskFailedAppError },
            { new TaskStateTransition(TaskState.TaskWaitingForClose, TaskEvent.FailedTaskSystemError), TaskState.TaskFailedSystemError },
            { new TaskStateTransition(TaskState.TaskWaitingForClose, TaskEvent.FailedTaskEvaluatorError), TaskState.TaskFailedByEvaluatorFailure },
            { new TaskStateTransition(TaskState.TaskWaitingForClose, TaskEvent.FailedTaskCommunicationError), TaskState.TaskFailedByGroupCommunication },
            { new TaskStateTransition(TaskState.TaskFailedSystemError, TaskEvent.FailedTaskEvaluatorError), TaskState.TaskFailedByEvaluatorFailure },
            { new TaskStateTransition(TaskState.TaskFailedByGroupCommunication, TaskEvent.FailedTaskEvaluatorError), TaskState.TaskFailedByEvaluatorFailure }
        };

        private readonly object _stateLock = new object();
        private TaskState _currentState;

        public DriverTaskState()
        {
            _currentState = TaskState.TaskNew;
        }

        public TaskState CurrentState
        {
            get
            {
                lock (_stateLock)
                {
                    return _currentState;
                }
            }
        }

        private TaskState GetNext(TaskEvent taskEvent)
        {
            TaskStateTransition transition = new TaskStateTransition(_currentState, taskEvent);
            TaskState nextState;
            if (!Transitions.TryGetValue(transition, out nextState))
            {
                throw new Exception("Invalid transition: " + _currentState + " -> " + taskEvent);
            }
            return nextState;
        }

        public TaskState MoveNext(TaskEvent command)
        {
            lock (_stateLock)
            {
                _currentState = GetNext(command);
                return _currentState;
            }
        }
    }
}
