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
    /// Events that trigger the states to transit from one to another
    /// </summary>
    internal enum SystemStateEvent
    {
        /// <summary>
        /// All active contexts are received
        /// </summary>
        AllContextsAreReady,

        /// <summary>
        /// All tasks are running
        /// </summary>
        AllTasksAreRunning,

        /// <summary>
        /// All tasks are completed
        /// </summary>
        AllTasksAreCompleted,
        
        /// <summary>
        /// A Task or Evaluator is failed
        /// </summary>
        FailedNode,

        /// <summary>
        /// System is not recoverable
        /// </summary>
        NotRecoverable,

        /// <summary>
        /// Ready to recover
        /// </summary>
        Recover
    }
}
