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
    /// Task states in task transitions
    /// </summary>
    public enum TaskState
    {
        /// <summary>
        /// When task (task configuration) is first created and added in the task list, its state is TaskNew
        /// </summary>
        TaskNew,

        /// <summary>
        /// After submitting a task with an IActiveContext, the task state is changed to TaskSubmittedg
        /// </summary>
        TaskSubmitted,

        /// <summary>
        /// After receiving RunningTask event, the task state is TaskRunning
        /// </summary>
        TaskRunning,

        /// <summary>
        /// After receiving CompletedTask event, the task state is TaskCompeleted
        /// </summary>
        TaskCompleted,

        /// <summary>
        /// After driver send command to close a task, the task state is set to TaskWaitingForClose
        /// </summary>
        TaskWaitingForClose,

        /// <summary>
        /// After receiving FailedTask event and verified it is closed by driver, the state is set to TaskClosedByDriver
        /// </summary>
        TaskClosedByDriver,

        /// <summary>
        /// After receiving FailedEvaluator event, set associated task state to TaskFailedByEvaluatorFailure
        /// </summary>
        TaskFailedByEvaluatorFailure,

        /// <summary>
        /// After receiving FailedTask event and verified it is caused by group communication
        /// </summary>
        TaskFailedByGroupCommunication,

        /// <summary>
        /// After receiving FailedTask event and verified it is caused by application error
        /// </summary>
        TaskFailedAppError,

        /// <summary>
        /// After receiving FailedTask event and verified it is caused by system error
        /// </summary>
        TaskFailedSystemError
    }
}
