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

using Org.Apache.REEF.IMRU.OnREEF.Driver.StateMachine;
using Xunit;

namespace Org.Apache.REEF.IMRU.Tests
{
    /// <summary>
    /// This is to test system state transition for all the possible scenarios
    /// </summary>
    public class TestSystemStates
    {
        /// <summary>
        /// Test Equal method for StateTransition
        /// </summary>
        [Fact]
        public void TestStateTransitionEquals()
        {
            var s1 = new StateTransition<SystemState, SystemStateEvent>(SystemState.WaitingForEvaluator,
                SystemStateEvent.AllContextsAreReady);
            var s2 = new StateTransition<SystemState, SystemStateEvent>(SystemState.WaitingForEvaluator,
                SystemStateEvent.AllContextsAreReady);
            Assert.True(s1.Equals(s2), "The two transitions should be equal");
        }

        /// <summary>
        /// Successful transitions from WaitingForEvaluator to TasksCompleted
        /// </summary>
        [Fact]
        public void TestFromRequestEvaluatorToTasksComplete()
        {
            var systemState = new SystemStateMachine();
            Assert.True(systemState.CurrentState.Equals(SystemState.WaitingForEvaluator), "The initial state should be WaitingForEvaluator.");
            Assert.True(systemState.MoveNext(SystemStateEvent.FailedNode).Equals(SystemState.WaitingForEvaluator), "Fail to stay at WaitingForEvaluator state.");
            Assert.True(systemState.MoveNext(SystemStateEvent.AllContextsAreReady).Equals(SystemState.SubmittingTasks), "Fail to move from WaitingForEvaluator state to SubmittingTasks state.");
            Assert.True(systemState.MoveNext(SystemStateEvent.AllTasksAreRunning).Equals(SystemState.TasksRunning), "Fail to move from SubmittingTasks state to TasksRunning state.");
            Assert.True(systemState.MoveNext(SystemStateEvent.AllTasksAreCompleted).Equals(SystemState.TasksCompleted), "Fail to move from TasksRunning state to TasksCompleted state.");
        }

        /// <summary>
        /// Fail scenario, from WaitingForEvaluator->Fail
        /// </summary>
        [Fact]
        public void TestNoRecoverableFailedEvaluatorDuringWaitingForEvaluator()
        {
            var systemState = new SystemStateMachine();
            Assert.True(systemState.MoveNext(SystemStateEvent.NotRecoverable).Equals(SystemState.Fail), "Fail to move from WaitingForEvaluator state to Fail state.");
        }

        /// <summary>
        /// Recovery scenario, from WaitingForEvaluator->SubmittingTasks->TasksRunning->ShuttingDown->WaitingForEvaluator
        /// </summary>
        [Fact]
        public void TestFailedNodeAfterTasksAreRunningThenRecovery()
        {
            var systemState = new SystemStateMachine();
            systemState.MoveNext(SystemStateEvent.AllContextsAreReady);
            systemState.MoveNext(SystemStateEvent.AllTasksAreRunning);
            Assert.True(systemState.MoveNext(SystemStateEvent.FailedNode).Equals(SystemState.ShuttingDown), "Fail to move from TasksRunning state to ShuttingDown state.");
            Assert.True(systemState.MoveNext(SystemStateEvent.FailedNode).Equals(SystemState.ShuttingDown), "Fail to stay at ShuttingDown state.");
            Assert.True(systemState.MoveNext(SystemStateEvent.Recover).Equals(SystemState.WaitingForEvaluator), "Fail to move from ShuttingDown state to WaitingForEvaluator state.");
        }

        /// <summary>
        /// Fail scenario, from WaitingForEvaluator->SubmittingTasks->TasksRunning->ShuttingDown->Fail
        /// </summary>
        [Fact]
        public void TestFailedNodeAfterTasksAreRunningThenFail()
        {
            var systemState = new SystemStateMachine();
            systemState.MoveNext(SystemStateEvent.AllContextsAreReady);
            systemState.MoveNext(SystemStateEvent.AllTasksAreRunning);
            Assert.True(systemState.MoveNext(SystemStateEvent.FailedNode).Equals(SystemState.ShuttingDown), "Fail to move from TasksRunning state to ShuttingDown state.");
            Assert.True(systemState.MoveNext(SystemStateEvent.NotRecoverable).Equals(SystemState.Fail), "Fail to move from ShuttingDown state to Fail state.");
        }

        /// <summary>
        /// Fail scenario, from WaitingForEvaluator->SubmittingTasks->ShuttingDown->Fail
        /// </summary>
        [Fact]
        public void TestFailedNodeDuringTaskSubmittingThenFail()
        {
            var systemState = new SystemStateMachine();
            systemState.MoveNext(SystemStateEvent.AllContextsAreReady);
            Assert.True(systemState.MoveNext(SystemStateEvent.FailedNode).Equals(SystemState.ShuttingDown), "Fail to move from SubmittingTasks state to ShuttingDown state.");
            Assert.True(systemState.MoveNext(SystemStateEvent.NotRecoverable).Equals(SystemState.Fail), "Fail to move from ShuttingDown state to Fail state.");
        }

        /// <summary>
        /// Recovery scenario, from WaitingForEvaluator->SubmittingTasks->ShuttingDown->WaitingForEvaluator
        /// </summary>
        [Fact]
        public void TestFailedNodeDuringTaskSubmittingThenRecovery()
        {
            var systemState = new SystemStateMachine();
            systemState.MoveNext(SystemStateEvent.AllContextsAreReady);
            Assert.True(systemState.MoveNext(SystemStateEvent.FailedNode).Equals(SystemState.ShuttingDown), "Fail to move from SubmittingTasks state to ShuttingDown state.");
            Assert.True(systemState.MoveNext(SystemStateEvent.Recover).Equals(SystemState.WaitingForEvaluator), "Fail to move from ShuttingDown state to WaitingForEvaluator state.");
        }
    }
}
