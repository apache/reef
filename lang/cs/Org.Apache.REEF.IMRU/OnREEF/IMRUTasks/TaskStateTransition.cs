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

namespace Org.Apache.REEF.IMRU.OnREEF.IMRUTasks
{
    /// <summary>
    /// It defines task state and task event combination that would determine the next transition state 
    /// </summary>
    internal sealed class TaskStateTransition
    {
        private readonly TaskState _currentState;
        private readonly TaskEvent _taskEvent;

        /// <summary>
        /// Constructor of the TaskStateTransition which wraps currentState and taskEvent
        /// </summary>
        /// <param name="currentState"></param>
        /// <param name="taskEvent"></param>
        internal TaskStateTransition(TaskState currentState, TaskEvent taskEvent)
        {
            _currentState = currentState;
            _taskEvent = taskEvent;
        }

        /// <summary>
        /// Hash code for the instance.
        /// It should be the same for the given object and as evenly distributed as possible within the integer range.
        /// One way to get a better distribution is to multiply a member by a prime number and add the next member, repeating as needed.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return 17 + (31 * _currentState.GetHashCode()) + (31 * _taskEvent.GetHashCode());
        }

        /// <summary>
        /// Checks if two TaskStateTransition objects have the same value of _currentState and _taskEvent
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            TaskStateTransition other = obj as TaskStateTransition;
            return other != null && _currentState == other._currentState && _taskEvent == other._taskEvent;
        }
    }
}
