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
using Org.Apache.REEF.IMRU.OnREEF.Driver.StateMachine;
using Xunit;

namespace Org.Apache.REEF.IMRU.Tests
{
    /// <summary>
    /// The test cases in this classes test TaskState and transitions
    /// </summary>
    public class TestTaskStates
    {
        /// <summary>
        /// This is to test a successful life cycle of task state transitions.
        /// For TaskNew->TaskSubmitted->TaskRunning->TaskCompleted
        /// </summary>
        [Fact]
        public void TestNewToCompleteScenario()
        {
            var taskState = new TaskStateMachine();
            Assert.True(taskState.CurrentState.Equals(TaskState.TaskNew), "State of newly created task should be TaskNew");
            Assert.False(taskState.IsFinalState(), "TaskNew should not be a final state.");
            Assert.True(taskState.MoveNext(TaskStateEvent.SubmittedTask).Equals(TaskState.TaskSubmitted), "Failed to move from TaskNew to TaskSubmitted state.");
            Assert.False(taskState.IsFinalState(), "TaskSubmitted should not be a final state.");
            Assert.True(taskState.MoveNext(TaskStateEvent.RunningTask).Equals(TaskState.TaskRunning), "Failed to move from TaskSubmitted to TaskRunning state.");
            Assert.False(taskState.IsFinalState(), "TaskRunning should not be a final state.");
            Assert.True(taskState.MoveNext(TaskStateEvent.CompletedTask).Equals(TaskState.TaskCompleted), "Failed to move from TaskRunning to TaskCompleted state.");
            Assert.True(taskState.IsFinalState(), "TaskCompleted should be a final state.");
        }

        /// <summary>
        /// This is to test a scenario from task running to close.
        /// </summary>
        [Fact]
        public void TestRunningToCloseScenario()
        {
            var taskState = new TaskStateMachine();
            taskState.MoveNext(TaskStateEvent.SubmittedTask);
            taskState.MoveNext(TaskStateEvent.RunningTask);
            Assert.True(taskState.MoveNext(TaskStateEvent.WaitingTaskToClose).Equals(TaskState.TaskWaitingForClose), "Failed to move from RunningTask to TaskWaitingForClose state.");
            Assert.False(taskState.IsFinalState(), "TaskWaitingForClose should not be a final state.");
            Assert.True(taskState.MoveNext(TaskStateEvent.ClosedTask).Equals(TaskState.TaskClosedByDriver), "Failed to move from TaskWaitingForClose to TaskClosedByDriver state.");
            Assert.True(taskState.IsFinalState(), "TaskClosedByDriver should be a final state.");
        }

        /// <summary>
        /// This is to test scenario from waiting for close and then get FailedTaskCommunicationError.
        /// </summary>
        [Fact]
        public void TestRunningToCloseToFailedTaskCommunicationErrorScenario()
        {
            var taskState = new TaskStateMachine();
            taskState.MoveNext(TaskStateEvent.SubmittedTask);
            taskState.MoveNext(TaskStateEvent.RunningTask);
            taskState.MoveNext(TaskStateEvent.WaitingTaskToClose);
            Assert.True(taskState.MoveNext(TaskStateEvent.FailedTaskCommunicationError).Equals(TaskState.TaskClosedByDriver), "Failed to move from WaitingTaskToClose to TaskClosedByDriver state with FailedTaskCommunicationError.");
        }

        /// <summary>
        /// This is to test scenario from waiting for close and then get FailedTaskAppError.
        /// </summary>
        [Fact]
        public void TestRunningToCloseToFailedTaskAppErrorScenario()
        {
            var taskState = new TaskStateMachine();
            taskState.MoveNext(TaskStateEvent.SubmittedTask);
            taskState.MoveNext(TaskStateEvent.RunningTask);
            taskState.MoveNext(TaskStateEvent.WaitingTaskToClose);
            Assert.True(taskState.MoveNext(TaskStateEvent.FailedTaskAppError).Equals(TaskState.TaskClosedByDriver), "Failed to move from WaitingTaskToClose to TaskClosedByDriver state with FailedTaskAppError.");
        }

        /// <summary>
        /// This is to test scenario from waiting for close and then get FailedTaskSystemError.
        /// </summary>
        [Fact]
        public void TestRunningToCloseToFailedTaskSystemErrorScenario()
        {
            var taskState = new TaskStateMachine();
            taskState.MoveNext(TaskStateEvent.SubmittedTask);
            taskState.MoveNext(TaskStateEvent.RunningTask);
            taskState.MoveNext(TaskStateEvent.WaitingTaskToClose);
            Assert.True(taskState.MoveNext(TaskStateEvent.FailedTaskSystemError).Equals(TaskState.TaskClosedByDriver), "Failed to move from WaitingTaskToClose to TaskClosedByDriver state with FailedTaskSystemError.");
        }

        /// <summary>
        /// This is to test scenario from waiting for close and then get FailedTaskEvaluatorError.
        /// </summary>
        [Fact]
        public void TestRunningToCloseToFailedTaskEvaluatorErrorScenario()
        {
            var taskState = new TaskStateMachine();
            taskState.MoveNext(TaskStateEvent.SubmittedTask);
            taskState.MoveNext(TaskStateEvent.RunningTask);
            taskState.MoveNext(TaskStateEvent.WaitingTaskToClose);
            Assert.True(taskState.MoveNext(TaskStateEvent.FailedTaskEvaluatorError).Equals(TaskState.TaskClosedByDriver), "Failed to move from WaitingTaskToClose to TaskClosedByDriver state with FailedTaskEvaluatorError.");
        }

        /// <summary>
        /// This is to test from WaitingTaskToClose to not allowed transitions.
        /// </summary>
        [Fact]
        public void TestRunningToWaitingTaskToCloseToNotAllowedTransitions()
        {
            var taskState = new TaskStateMachine();
            taskState.MoveNext(TaskStateEvent.SubmittedTask);
            taskState.MoveNext(TaskStateEvent.RunningTask);
            taskState.MoveNext(TaskStateEvent.WaitingTaskToClose);

            Action moveNext = () => taskState.MoveNext(TaskStateEvent.RunningTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskStateEvent.CompletedTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskStateEvent.SubmittedTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);
        }

        /// <summary>
        /// This is to test from RunningTask to TaskFailedByEvaluatorFailure.
        /// </summary>
        [Fact]
        public void TestRunningToFailByEvaluatorScenario()
        {
            var taskState = new TaskStateMachine();
            taskState.MoveNext(TaskStateEvent.SubmittedTask);
            taskState.MoveNext(TaskStateEvent.RunningTask);
            Assert.True(taskState.MoveNext(TaskStateEvent.FailedTaskEvaluatorError).Equals(TaskState.TaskFailedByEvaluatorFailure), "Failed to move from RunningTask to TaskFailedByEvaluatorFailure state with FailedTaskEvaluatorError.");
            Assert.True(taskState.IsFinalState(), "TaskFailedByEvaluatorFailure should be a final state.");
        }

        /// <summary>
        /// This is to test from RunningTask to TaskFailedByGroupCommunication and then TaskFailedByEvaluatorFailure.
        /// </summary>
        [Fact]
        public void TestRunningToFailByCommunicationThenByEvaluatorScenario()
        {
            var taskState = new TaskStateMachine();
            taskState.MoveNext(TaskStateEvent.SubmittedTask);
            taskState.MoveNext(TaskStateEvent.RunningTask);
            Assert.True(taskState.MoveNext(TaskStateEvent.FailedTaskCommunicationError).Equals(TaskState.TaskFailedByGroupCommunication), "Failed to move from RunningTask to TaskFailedByGroupCommunication state with FailedTaskCommunicationError.");
            Assert.True(taskState.IsFinalState(), "TaskFailedByGroupCommunication should be a final state.");
            Assert.True(taskState.MoveNext(TaskStateEvent.FailedTaskEvaluatorError).Equals(TaskState.TaskFailedByEvaluatorFailure), "Failed to move from TaskFailedByGroupCommunication to TaskFailedByEvaluatorFailure state with FailedTaskEvaluatorError.");
            Assert.True(taskState.IsFinalState(), "TaskFailedByEvaluatorFailure should be a final state.");
        }

        /// <summary>
        /// This is to test from RunningTask to TaskFailedByGroupCommunication and then TaskFailedByEvaluatorFailure.
        /// </summary>
        [Fact]
        public void TestRunningToFailBySystemThenByEvaluatorScenario()
        {
            var taskState = new TaskStateMachine();
            taskState.MoveNext(TaskStateEvent.SubmittedTask);
            taskState.MoveNext(TaskStateEvent.RunningTask);
            Assert.True(taskState.MoveNext(TaskStateEvent.FailedTaskSystemError).Equals(TaskState.TaskFailedBySystemError), "Failed to move from RunningTask to TaskFailedBySystemError state with FailedTaskSystemError.");
            Assert.True(taskState.IsFinalState(), "TaskFailedByGroupCommunication should be a final state.");
            Assert.True(taskState.MoveNext(TaskStateEvent.FailedTaskEvaluatorError).Equals(TaskState.TaskFailedByEvaluatorFailure), "Failed to move from TaskFailedBySystemError to TaskFailedByEvaluatorFailure state with FailedTaskEvaluatorError.");
            Assert.True(taskState.IsFinalState(), "TaskFailedByEvaluatorFailure should be a final state.");
        }

        /// <summary>
        /// This is to test from TaskNew to not allowed transitions
        /// </summary>
        [Fact]
        public void TestFromNewToNotAllowedTransitions()
        {
            var taskState = new TaskStateMachine();
            
            Action moveNext = () => taskState.MoveNext(TaskStateEvent.RunningTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskStateEvent.WaitingTaskToClose);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskStateEvent.CompletedTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);
        }

        /// <summary>
        /// This is to test from RunningTask to not allowed transitions
        /// </summary>
        [Fact]
        public void TestFromRunningToNotAllowedTransitions()
        {
            var taskState = new TaskStateMachine();
            taskState.MoveNext(TaskStateEvent.SubmittedTask);
            taskState.MoveNext(TaskStateEvent.RunningTask);

            Action moveNext = () => taskState.MoveNext(TaskStateEvent.SubmittedTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskStateEvent.ClosedTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);
        }

        /// <summary>
        /// This is to test from FailedTaskEvaluatorError to not allowed transitions
        /// </summary>
        [Fact]
        public void TestFromFailToNotAllowedTransitions()
        {
            var taskState = new TaskStateMachine();
            taskState.MoveNext(TaskStateEvent.SubmittedTask);
            taskState.MoveNext(TaskStateEvent.RunningTask);

            taskState.MoveNext(TaskStateEvent.FailedTaskEvaluatorError);
            Assert.True(taskState.CurrentState.Equals(TaskState.TaskFailedByEvaluatorFailure), "Failed to move to TaskFailedByEvaluatorFailure state.");

            Action moveNext = () => taskState.MoveNext(TaskStateEvent.RunningTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskStateEvent.ClosedTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskStateEvent.CompletedTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskStateEvent.SubmittedTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskStateEvent.WaitingTaskToClose);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskStateEvent.FailedTaskAppError);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskStateEvent.FailedTaskCommunicationError);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskStateEvent.FailedTaskSystemError);
            Assert.Throws<TaskStateTransitionException>(moveNext); 
        }
    }
}
