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
    /// Task events that triggers task state transition
    /// </summary>
    public enum TaskEvent
    {
        /// <summary>
        /// Task is submitted
        /// </summary>
        SubmittedTask,

        /// <summary>
        /// Task is running
        /// </summary>
        RunningTask,

        /// <summary>
        /// Task is completed
        /// </summary>
        CompletedTask,

        /// <summary>
        /// Waiting for task is closed
        /// </summary>
        WaitingTaskToClose,

        /// <summary>
        /// Task is closed
        /// </summary>
        ClosedTask,

        /// <summary>
        /// Received failed task with application error
        /// </summary>
        FailedTaskAppError,

        /// <summary>
        /// Received failed task with system error
        /// </summary>
        FailedTaskSystemError,

        /// <summary>
        /// Received failed Evaluator that caused associated task fail
        /// </summary>
        FailedTaskEvaluatorError,

        /// <summary>
        /// Received failed task with communication error
        /// </summary>
        FailedTaskCommunicationError
    }
}
