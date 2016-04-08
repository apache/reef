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
        public void TestHappySenario()
        {
            var taskState = new DriverTaskState();
            Assert.True(taskState.CurrentState.Equals(TaskStates.TaskNew));
            Assert.True(taskState.MoveNext(TaskEvents.SubmittingTask).Equals(TaskStates.TaskSubmitting));
            Assert.True(taskState.MoveNext(TaskEvents.RunningTask).Equals(TaskStates.TaskRunning));
            Assert.True(taskState.MoveNext(TaskEvents.CompleteTask).Equals(TaskStates.TaskCompeleted));
        }

        [Fact]
        public void TestRunningToCloseSenario()
        {
            var taskState = new DriverTaskState();
            Assert.True(taskState.CurrentState.Equals(TaskStates.TaskNew));
            Assert.True(taskState.MoveNext(TaskEvents.SubmittingTask).Equals(TaskStates.TaskSubmitting));
            Assert.True(taskState.MoveNext(TaskEvents.RunningTask).Equals(TaskStates.TaskRunning));
            Assert.True(taskState.MoveNext(TaskEvents.WaitingTaskToClose).Equals(TaskStates.TaskWaitingForClose));
            Assert.True(taskState.MoveNext(TaskEvents.CloseTask).Equals(TaskStates.TaskClosedByDriver));
        }

        [Fact]
        public void TestRunningToFailByEvaluator()
        {
            var taskState = new DriverTaskState();
            Assert.True(taskState.CurrentState.Equals(TaskStates.TaskNew));
            Assert.True(taskState.MoveNext(TaskEvents.SubmittingTask).Equals(TaskStates.TaskSubmitting));
            Assert.True(taskState.MoveNext(TaskEvents.RunningTask).Equals(TaskStates.TaskRunning));
            Assert.True(taskState.MoveNext(TaskEvents.FaileTaskEvaluatorError).Equals(TaskStates.TaskFailedByEvaluatorFailure));
        }

        [Fact]
        public void TestRunningToFailByCommuThenEvaluator()
        {
            var taskState = new DriverTaskState();
            Assert.True(taskState.CurrentState.Equals(TaskStates.TaskNew));
            Assert.True(taskState.MoveNext(TaskEvents.SubmittingTask).Equals(TaskStates.TaskSubmitting));
            Assert.True(taskState.MoveNext(TaskEvents.RunningTask).Equals(TaskStates.TaskRunning));
            Assert.True(taskState.MoveNext(TaskEvents.FailTaskCommuError).Equals(TaskStates.TaskFailedByGroupCommunication));
            Assert.True(taskState.MoveNext(TaskEvents.FaileTaskEvaluatorError).Equals(TaskStates.TaskFailedByEvaluatorFailure));
        }

        [Fact]
        public void TestFromNewToNotAllowed()
        {
            var taskState = new DriverTaskState();
            Assert.True(taskState.CurrentState.Equals(TaskStates.TaskNew));
            try
            {
                taskState.MoveNext(TaskEvents.RunningTask);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvents.WaitingTaskToClose);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvents.CompleteTask);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }
        }

        [Fact]
        public void TestFromRunningToNotAllowed()
        {
            var taskState = new DriverTaskState();
            Assert.True(taskState.CurrentState.Equals(TaskStates.TaskNew));
            taskState.MoveNext(TaskEvents.SubmittingTask);
            Assert.True(taskState.CurrentState.Equals(TaskStates.TaskSubmitting));
            taskState.MoveNext(TaskEvents.RunningTask);
            Assert.True(taskState.CurrentState.Equals(TaskStates.TaskRunning));

            try
            {
                taskState.MoveNext(TaskEvents.SubmittingTask);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvents.CloseTask);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }
        }

        [Fact]
        public void TestFromFailToNotAllowed()
        {
            var taskState = new DriverTaskState();
            Assert.True(taskState.CurrentState.Equals(TaskStates.TaskNew));
            taskState.MoveNext(TaskEvents.SubmittingTask);
            Assert.True(taskState.CurrentState.Equals(TaskStates.TaskSubmitting));
            taskState.MoveNext(TaskEvents.RunningTask);
            Assert.True(taskState.CurrentState.Equals(TaskStates.TaskRunning));
            taskState.MoveNext(TaskEvents.FaileTaskEvaluatorError);
            Assert.True(taskState.CurrentState.Equals(TaskStates.TaskFailedByEvaluatorFailure));

            try
            {
                taskState.MoveNext(TaskEvents.RunningTask);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvents.CloseTask);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvents.CompleteTask);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvents.SubmittingTask);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvents.WaitingTaskToClose);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvents.FailTaskAppError);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvents.FailTaskCommuError);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvents.FailTaskSystemError);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }
        }
    }
}
