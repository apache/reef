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

namespace Org.Apache.REEF.IMRU.OnREEF.Driver.StateMachine
{
    /// <summary>
    /// System states during a complete life cycle 
    /// </summary>
    internal enum SystemState
    {
        /// <summary>
        /// In this state, the driver is waiting for Evaluators to be allocated and for data to be loaded into them. 
        /// This is the state the Driver starts in. It is also the state the Driver is in recovery after suffering a failure. 
        /// </summary>
        WaitingForEvaluator,
        
        /// <summary>
        /// In this state, the driver submits all the tasks to the contexts. At this state, driver creates a communication group 
        /// and its topology. Once it has all the expected tasks, it enters the TasksRunning state. During this stage, if there 
        /// is any failed task or failed evaluator, it goes to ShuttingDown state.
        /// </summary>
        SubmittingTasks,

        /// <summary>
        /// This is the sate after all tasks are running. If we receive a task failure or evaluator failure in this state, it goes to ShuttingDown state. 
        /// </summary>
        TasksRunning,

        /// <summary>
        /// After all tasks are completed, the state will be set to this state. And then all the active contexts will be closed. 
        /// </summary>
        TasksCompleted,

        /// <summary>
        /// If an evaluator failed or Task failed, it enters this state.
        /// </summary>
        ShuttingDown,

        /// <summary>
        /// Error happens and the system is not recoverable, it enters Fail state. 
        /// </summary>
        Fail
    }
}