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

using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Driver.Default
{
    /// <summary>
    /// Definition of the the different states in which a task can be.
    /// </summary>
    internal enum TaskState
    {
        Init = 1,

        Queued = 2,

        Submitted = 3,

        Recovering = 4,

        Running = 5,

        Failed = 6,

        Completed = 7
    }

    /// <summary>
    /// Utility class used to recognize particular task states.
    /// </summary>
    internal static class TaskStateUtils
    {
        private static readonly TaskState[] Recoverable = { TaskState.Failed, TaskState.Queued };

        private static readonly TaskState[] NotRunnable = { TaskState.Failed, TaskState.Completed };

        /// <summary>
        /// Whether a task is recoverable or not.
        /// </summary>
        /// <param name="state">The current state of the task</param>
        /// <returns>True if the task is recoverable</returns>
        public static bool IsRecoverable(this TaskState state)
        {
            return Recoverable.Contains(state);
        }

        /// <summary>
        /// Whether a task can be run or not.
        /// </summary>
        /// <param name="state">The current state of the task</param>
        /// <returns>True if the task can be run</returns>
        public static bool IsRunnable(this TaskState state)
        {
            return !NotRunnable.Contains(state);
        }
    }
}
