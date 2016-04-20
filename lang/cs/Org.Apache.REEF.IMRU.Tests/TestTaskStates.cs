﻿// Licensed to the Apache Software Foundation (ASF) under one
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
using Org.Apache.REEF.IMRU.OnREEF.Exceptions;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
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
        public void TestNewToCompleteSenario()
        {
            var taskState = new DriverTaskState();
            Assert.True(taskState.CurrentState.Equals(TaskState.TaskNew));
            Assert.False(taskState.IsFinalState(), "TaskNew should not be a final state.");
            Assert.True(taskState.MoveNext(TaskEvent.SubmittedTask).Equals(TaskState.TaskSubmitted), "Fail to move to TaskSubmitting state.");
            Assert.False(taskState.IsFinalState(), "TaskSubmitting should not be a final state."); 
            Assert.True(taskState.MoveNext(TaskEvent.RunningTask).Equals(TaskState.TaskRunning), "Fail to move to TaskRunning state.");
            Assert.False(taskState.IsFinalState(), "TaskRunning should not be a final state."); 
            Assert.True(taskState.MoveNext(TaskEvent.CompletedTask).Equals(TaskState.TaskCompleted), "Fail to move to TaskCompleted state.");
            Assert.True(taskState.IsFinalState(), "TaskCompleted should be a final state.");
        }

        /// <summary>
        /// This is to test a scenario from task running to close.
        /// </summary>
        [Fact]
        public void TestRunningToCloseSenario()
        {
            var taskState = new DriverTaskState();
            taskState.MoveNext(TaskEvent.SubmittedTask);
            taskState.MoveNext(TaskEvent.RunningTask);
            Assert.True(taskState.MoveNext(TaskEvent.WaitingTaskToClose).Equals(TaskState.TaskWaitingForClose), "Fail to move to TaskWaitingForClose state.");
            Assert.False(taskState.IsFinalState(), "TaskWaitingForClose should not be a final state."); 
            Assert.True(taskState.MoveNext(TaskEvent.ClosedTask).Equals(TaskState.TaskClosedByDriver), "Fail to move to TaskClosedByDriver state.");
            Assert.True(taskState.IsFinalState(), "TaskClosedByDriver should be a final state.");
        }

        /// <summary>
        /// This is to test scenario from waiting for close and then get FailedTaskCommunicationError.
        /// </summary>
        [Fact]
        public void TestRunningToCloseToFailTaskCommuniSenario()
        {
            var taskState = new DriverTaskState();
            taskState.MoveNext(TaskEvent.SubmittedTask);
            taskState.MoveNext(TaskEvent.RunningTask);
            taskState.MoveNext(TaskEvent.WaitingTaskToClose);
            Assert.True(taskState.MoveNext(TaskEvent.FailedTaskCommunicationError).Equals(TaskState.TaskClosedByDriver), "Fail to move to TaskClosedByDriver state.");
        }

        /// <summary>
        /// This is to test scenario from waiting for close and then get FailedTaskAppError.
        /// </summary>
        [Fact]
        public void TestRunningToCloseToFailTaskAppSenario()
        {
            var taskState = new DriverTaskState();
            taskState.MoveNext(TaskEvent.SubmittedTask);
            taskState.MoveNext(TaskEvent.RunningTask);
            taskState.MoveNext(TaskEvent.WaitingTaskToClose);
            Assert.True(taskState.MoveNext(TaskEvent.FailedTaskAppError).Equals(TaskState.TaskClosedByDriver), "Fail to move to TaskClosedByDriver state.");
        }

        /// <summary>
        /// This is to test from WaitingTaskToClose to not allowed transition.
        /// </summary>
        [Fact]
        public void TestRunningToCloseAndFail()
        {
            var taskState = new DriverTaskState();
            taskState.MoveNext(TaskEvent.SubmittedTask);
            taskState.MoveNext(TaskEvent.RunningTask);
            taskState.MoveNext(TaskEvent.WaitingTaskToClose);

            Action moveNext = () => taskState.MoveNext(TaskEvent.RunningTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.CompletedTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);
        }

        /// <summary>
        /// This is to test from RunningTask to TaskFailedByEvaluatorFailure.
        /// </summary>
        [Fact]
        public void TestRunningToFailByEvaluator()
        {
            var taskState = new DriverTaskState();
            taskState.MoveNext(TaskEvent.SubmittedTask);
            taskState.MoveNext(TaskEvent.RunningTask);
            Assert.True(taskState.MoveNext(TaskEvent.FailedTaskEvaluatorError).Equals(TaskState.TaskFailedByEvaluatorFailure), "Fail to move to TaskFailedByEvaluatorFailure state.");
            Assert.True(taskState.IsFinalState(), "TaskFailedByEvaluatorFailure should be a final state.");
        }

        /// <summary>
        /// This is to test from RunningTask to TaskFailedByGroupCommunication, then TaskFailedByEvaluatorFailure.
        /// </summary>
        [Fact]
        public void TestRunningToFailByCommuThenEvaluator()
        {
            var taskState = new DriverTaskState();
            taskState.MoveNext(TaskEvent.SubmittedTask);
            taskState.MoveNext(TaskEvent.RunningTask);
            Assert.True(taskState.MoveNext(TaskEvent.FailedTaskCommunicationError).Equals(TaskState.TaskFailedByGroupCommunication), "Fail to move to TaskFailedByGroupCommunication state.");
            Assert.True(taskState.IsFinalState(), "TaskFailedByGroupCommunication should be a final state.");
            Assert.True(taskState.MoveNext(TaskEvent.FailedTaskEvaluatorError).Equals(TaskState.TaskFailedByEvaluatorFailure), "Fail to move to TaskFailedByEvaluatorFailure state.");
            Assert.True(taskState.IsFinalState(), "TaskFailedByEvaluatorFailure should be a final state.");
        }

        /// <summary>
        /// This is to test from TaskNew to not allowed transitions
        /// </summary>
        [Fact]
        public void TestFromNewToNotAllowed()
        {
            var taskState = new DriverTaskState();
            
            Action moveNext = () => taskState.MoveNext(TaskEvent.RunningTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.WaitingTaskToClose);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.CompletedTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);
        }

        /// <summary>
        /// This is to test from RunningTask to not allowed transitions
        /// </summary>
        [Fact]
        public void TestFromRunningToNotAllowed()
        {
            var taskState = new DriverTaskState();
            taskState.MoveNext(TaskEvent.SubmittedTask);
            taskState.MoveNext(TaskEvent.RunningTask);

            Action moveNext = () => taskState.MoveNext(TaskEvent.SubmittedTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.ClosedTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);
        }

        /// <summary>
        /// This is to test from FailedTaskEvaluatorError to not allowed transitions
        /// </summary>
        [Fact]
        public void TestFromFailToNotAllowed()
        {
            var taskState = new DriverTaskState();
            taskState.MoveNext(TaskEvent.SubmittedTask);
            taskState.MoveNext(TaskEvent.RunningTask);

            taskState.MoveNext(TaskEvent.FailedTaskEvaluatorError);
            Assert.True(taskState.CurrentState.Equals(TaskState.TaskFailedByEvaluatorFailure), "Fail to move to TaskFailedByEvaluatorFailure state.");

            Action moveNext = () => taskState.MoveNext(TaskEvent.RunningTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.ClosedTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.CompletedTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.SubmittedTask);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.WaitingTaskToClose);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.FailedTaskAppError);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.FailedTaskCommunicationError);
            Assert.Throws<TaskStateTransitionException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.FailedTaskSystemError);
            Assert.Throws<TaskStateTransitionException>(moveNext); 
        }
    }
}
