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
using Org.Apache.REEF.Driver.Task;
using Xunit;

namespace Org.Apache.REEF.Driver.Tests
{
    /// <summary>
    /// The test cases in this classes test TaskState and transitions
    /// </summary>
    public class TestTaskStates
    {
        [Fact]
        public void TestNewToCompleteSenario()
        {
            var taskState = new DriverTaskState();
            Assert.True(taskState.CurrentState.Equals(TaskTransitionState.TaskNew));
            Assert.False(taskState.IsFinalState(), "TaskNew is not final state.");
            Assert.True(taskState.MoveNext(TaskEvent.SubmittedTask).Equals(TaskTransitionState.TaskSubmitting), "Fail to move to TaskSubmitting state.");
            Assert.False(taskState.IsFinalState(), "TaskSubmitting is not final state."); 
            Assert.True(taskState.MoveNext(TaskEvent.RunningTask).Equals(TaskTransitionState.TaskRunning), "Fail to move to TaskRunning state.");
            Assert.False(taskState.IsFinalState(), "TaskRunning is not final state."); 
            Assert.True(taskState.MoveNext(TaskEvent.CompletedTask).Equals(TaskTransitionState.TaskCompleted), "Fail to move to TaskCompleted state.");
            Assert.True(taskState.IsFinalState(), "TaskCompleted should be a final state.");
        }

        [Fact]
        public void TestRunningToCloseSenario()
        {
            var taskState = new DriverTaskState();
            taskState.MoveNext(TaskEvent.SubmittedTask);
            taskState.MoveNext(TaskEvent.RunningTask);
            Assert.True(taskState.MoveNext(TaskEvent.WaitingTaskToClose).Equals(TaskTransitionState.TaskWaitingForClose), "Fail to move to TaskWaitingForClose state.");
            Assert.False(taskState.IsFinalState(), "TaskWaitingForClose is not final state."); 
            Assert.True(taskState.MoveNext(TaskEvent.ClosedTask).Equals(TaskTransitionState.TaskClosedByDriver), "Fail to move to TaskClosedByDriver state.");
            Assert.True(taskState.IsFinalState(), "TaskClosedByDriver should be a final state.");
        }

        [Fact]
        public void TestRunningToFailByEvaluator()
        {
            var taskState = new DriverTaskState();
            taskState.MoveNext(TaskEvent.SubmittedTask);
            taskState.MoveNext(TaskEvent.RunningTask);
            Assert.True(taskState.MoveNext(TaskEvent.FailedTaskEvaluatorError).Equals(TaskTransitionState.TaskFailedByEvaluatorFailure), "Fail to move to TaskFailedByEvaluatorFailure state.");
            Assert.True(taskState.IsFinalState(), "TaskFailedByEvaluatorFailure should be a final state.");
        }

        [Fact]
        public void TestRunningToFailByCommuThenEvaluator()
        {
            var taskState = new DriverTaskState();
            taskState.MoveNext(TaskEvent.SubmittedTask);
            taskState.MoveNext(TaskEvent.RunningTask);
            Assert.True(taskState.MoveNext(TaskEvent.FailedTaskCommunicationError).Equals(TaskTransitionState.TaskFailedByGroupCommunication), "Fail to move to TaskFailedByGroupCommunication state.");
            Assert.True(taskState.IsFinalState(), "TaskFailedByGroupCommunication should be a final state.");
            Assert.True(taskState.MoveNext(TaskEvent.FailedTaskEvaluatorError).Equals(TaskTransitionState.TaskFailedByEvaluatorFailure), "Fail to move to TaskFailedByEvaluatorFailure state.");
            Assert.True(taskState.IsFinalState(), "TaskFailedByEvaluatorFailure should be a final state.");
        }

        [Fact]
        public void TestFromNewToNotAllowed()
        {
            var taskState = new DriverTaskState();
            Assert.True(taskState.CurrentState.Equals(TaskTransitionState.TaskNew));
            
            Action moveNext = () => taskState.MoveNext(TaskEvent.RunningTask);
            Assert.Throws<ApplicationException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.WaitingTaskToClose);
            Assert.Throws<ApplicationException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.CompletedTask);
            Assert.Throws<ApplicationException>(moveNext);
        }

        [Fact]
        public void TestFromRunningToNotAllowed()
        {
            var taskState = new DriverTaskState();
            Assert.True(taskState.CurrentState.Equals(TaskTransitionState.TaskNew), "Task initial state is not TaskNew.");

            taskState.MoveNext(TaskEvent.SubmittedTask);
            Assert.True(taskState.CurrentState.Equals(TaskTransitionState.TaskSubmitting), "Fail to move to TaskSubmitting state.");

            taskState.MoveNext(TaskEvent.RunningTask);
            Assert.True(taskState.CurrentState.Equals(TaskTransitionState.TaskRunning), "Fail to move to TaskRunning state.");

            Action moveNext = () => taskState.MoveNext(TaskEvent.SubmittedTask);
            Assert.Throws<ApplicationException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.ClosedTask);
            Assert.Throws<ApplicationException>(moveNext);
        }

        [Fact]
        public void TestFromFailToNotAllowed()
        {
            var taskState = new DriverTaskState();
            Assert.True(taskState.CurrentState.Equals(TaskTransitionState.TaskNew), "Task initial state is not TaskNew.");

            taskState.MoveNext(TaskEvent.SubmittedTask);
            Assert.True(taskState.CurrentState.Equals(TaskTransitionState.TaskSubmitting), "Fail to move to TaskSubmitting state.");

            taskState.MoveNext(TaskEvent.RunningTask);
            Assert.True(taskState.CurrentState.Equals(TaskTransitionState.TaskRunning), "Fail to move to TaskRunning state.");

            taskState.MoveNext(TaskEvent.FailedTaskEvaluatorError);
            Assert.True(taskState.CurrentState.Equals(TaskTransitionState.TaskFailedByEvaluatorFailure), "Fail to move to TaskFailedByEvaluatorFailure state.");

            Action moveNext = () => taskState.MoveNext(TaskEvent.RunningTask);
            Assert.Throws<ApplicationException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.ClosedTask);
            Assert.Throws<ApplicationException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.CompletedTask);
            Assert.Throws<ApplicationException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.SubmittedTask);
            Assert.Throws<ApplicationException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.WaitingTaskToClose);
            Assert.Throws<ApplicationException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.FailedTaskAppError);
            Assert.Throws<ApplicationException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.FailedTaskCommunicationError);
            Assert.Throws<ApplicationException>(moveNext);

            moveNext = () => taskState.MoveNext(TaskEvent.FailedTaskSystemError);
            Assert.Throws<ApplicationException>(moveNext); 
        }
    }
}
