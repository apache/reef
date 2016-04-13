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

namespace Org.Apache.REEF.Driver.Task
{
    /// <summary>
    /// It defines task state and task event combination that would determine the next transition state 
    /// </summary>
    public sealed class TaskStateTransition
    {
        private readonly TaskTransitionState _currentState;
        private readonly TaskEvent _taskEvent;

        public TaskStateTransition(TaskTransitionState currentState, TaskEvent taskEvent)
        {
            _currentState = currentState;
            _taskEvent = taskEvent;
        }

        public override int GetHashCode()
        {
            return 17 + (31 * _currentState.GetHashCode()) + (31 * _taskEvent.GetHashCode());
        }

        public override bool Equals(object obj)
        {
            TaskStateTransition other = obj as TaskStateTransition;
            return other != null && _currentState == other._currentState && _taskEvent == other._taskEvent;
        }
    }
}
