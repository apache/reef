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
    public class DriverTaskState
    {
        internal static IDictionary<TaskStateTransition, TaskStates> Transitions = new Dictionary<TaskStateTransition, TaskStates>
        {
            { new TaskStateTransition(TaskStates.TaskNew, TaskEvents.SubmittingTask), TaskStates.TaskSubmitting },
            { new TaskStateTransition(TaskStates.TaskNew, TaskEvents.CloseTask), TaskStates.TaskClosedByDriver },            
            { new TaskStateTransition(TaskStates.TaskSubmitting, TaskEvents.RunningTask), TaskStates.TaskRunning },
            { new TaskStateTransition(TaskStates.TaskSubmitting, TaskEvents.FailTaskAppError), TaskStates.TaskFailedAppError },
            { new TaskStateTransition(TaskStates.TaskSubmitting, TaskEvents.FailTaskSystemError), TaskStates.TaskFailedSystemError },
            { new TaskStateTransition(TaskStates.TaskSubmitting, TaskEvents.FaileTaskEvaluatorError), TaskStates.TaskFailedByEvaluatorFailure },
            { new TaskStateTransition(TaskStates.TaskSubmitting, TaskEvents.FailTaskCommuError), TaskStates.TaskFailedByGroupCommunication },
            { new TaskStateTransition(TaskStates.TaskRunning, TaskEvents.CompleteTask), TaskStates.TaskCompeleted },
            { new TaskStateTransition(TaskStates.TaskRunning, TaskEvents.WaitingTaskToClose), TaskStates.TaskWaitingForClose },
            { new TaskStateTransition(TaskStates.TaskRunning, TaskEvents.FailTaskAppError), TaskStates.TaskFailedAppError },
            { new TaskStateTransition(TaskStates.TaskRunning, TaskEvents.FailTaskSystemError), TaskStates.TaskFailedSystemError },
            { new TaskStateTransition(TaskStates.TaskRunning, TaskEvents.FaileTaskEvaluatorError), TaskStates.TaskFailedByEvaluatorFailure },
            { new TaskStateTransition(TaskStates.TaskRunning, TaskEvents.FailTaskCommuError), TaskStates.TaskFailedByGroupCommunication },
            { new TaskStateTransition(TaskStates.TaskWaitingForClose, TaskEvents.CloseTask), TaskStates.TaskClosedByDriver },
            { new TaskStateTransition(TaskStates.TaskWaitingForClose, TaskEvents.FailTaskAppError), TaskStates.TaskFailedAppError },
            { new TaskStateTransition(TaskStates.TaskWaitingForClose, TaskEvents.FailTaskSystemError), TaskStates.TaskFailedSystemError },
            { new TaskStateTransition(TaskStates.TaskWaitingForClose, TaskEvents.FaileTaskEvaluatorError), TaskStates.TaskFailedByEvaluatorFailure },
            { new TaskStateTransition(TaskStates.TaskWaitingForClose, TaskEvents.FailTaskCommuError), TaskStates.TaskFailedByGroupCommunication },
            { new TaskStateTransition(TaskStates.TaskFailedSystemError, TaskEvents.FaileTaskEvaluatorError), TaskStates.TaskFailedByEvaluatorFailure },
            { new TaskStateTransition(TaskStates.TaskFailedByGroupCommunication, TaskEvents.FaileTaskEvaluatorError), TaskStates.TaskFailedByEvaluatorFailure }
        };

        private readonly object _stateLock = new object();
        private TaskStates _currentState;

        public DriverTaskState()
        {
            _currentState = TaskStates.TaskNew;
        }

        public TaskStates CurrentState
        {
            get
            {
                lock (_stateLock)
                {
                    return _currentState;
                }
            }
        }

        public TaskStates GetNext(TaskEvents taskEvent)
        {
            TaskStateTransition transition = new TaskStateTransition(_currentState, taskEvent);
            TaskStates nextState;
            if (!Transitions.TryGetValue(transition, out nextState))
            {
                throw new Exception("Invalid transition: " + _currentState + " -> " + taskEvent);
            }
            return nextState;
        }

        public TaskStates MoveNext(TaskEvents command)
        {
            lock (_stateLock)
            {
                _currentState = GetNext(command);
                return _currentState;
            }
        }
    }
}
