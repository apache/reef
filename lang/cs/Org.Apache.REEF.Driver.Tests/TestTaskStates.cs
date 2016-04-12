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
            Assert.True(taskState.CurrentState.Equals(TaskState.TaskNew));
            Assert.True(taskState.MoveNext(TaskEvent.SubmittedTask).Equals(TaskState.TaskSubmitting));
            Assert.True(taskState.MoveNext(TaskEvent.RunningTask).Equals(TaskState.TaskRunning));
            Assert.True(taskState.MoveNext(TaskEvent.CompletedTask).Equals(TaskState.TaskCompeleted));
        }

        [Fact]
        public void TestRunningToCloseSenario()
        {
            var taskState = new DriverTaskState();
            Assert.True(taskState.CurrentState.Equals(TaskState.TaskNew));
            Assert.True(taskState.MoveNext(TaskEvent.SubmittedTask).Equals(TaskState.TaskSubmitting));
            Assert.True(taskState.MoveNext(TaskEvent.RunningTask).Equals(TaskState.TaskRunning));
            Assert.True(taskState.MoveNext(TaskEvent.WaitingTaskToClose).Equals(TaskState.TaskWaitingForClose));
            Assert.True(taskState.MoveNext(TaskEvent.ClosedTask).Equals(TaskState.TaskClosedByDriver));
        }

        [Fact]
        public void TestRunningToFailByEvaluator()
        {
            var taskState = new DriverTaskState();
            Assert.True(taskState.CurrentState.Equals(TaskState.TaskNew));
            Assert.True(taskState.MoveNext(TaskEvent.SubmittedTask).Equals(TaskState.TaskSubmitting));
            Assert.True(taskState.MoveNext(TaskEvent.RunningTask).Equals(TaskState.TaskRunning));
            Assert.True(taskState.MoveNext(TaskEvent.FailedTaskEvaluatorError).Equals(TaskState.TaskFailedByEvaluatorFailure));
        }

        [Fact]
        public void TestRunningToFailByCommuThenEvaluator()
        {
            var taskState = new DriverTaskState();
            Assert.True(taskState.CurrentState.Equals(TaskState.TaskNew));
            Assert.True(taskState.MoveNext(TaskEvent.SubmittedTask).Equals(TaskState.TaskSubmitting));
            Assert.True(taskState.MoveNext(TaskEvent.RunningTask).Equals(TaskState.TaskRunning));
            Assert.True(taskState.MoveNext(TaskEvent.FailedTaskCommunicationError).Equals(TaskState.TaskFailedByGroupCommunication));
            Assert.True(taskState.MoveNext(TaskEvent.FailedTaskEvaluatorError).Equals(TaskState.TaskFailedByEvaluatorFailure));
        }

        [Fact]
        public void TestFromNewToNotAllowed()
        {
            var taskState = new DriverTaskState();
            Assert.True(taskState.CurrentState.Equals(TaskState.TaskNew));
            try
            {
                taskState.MoveNext(TaskEvent.RunningTask);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvent.WaitingTaskToClose);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvent.CompletedTask);
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
            Assert.True(taskState.CurrentState.Equals(TaskState.TaskNew));
            taskState.MoveNext(TaskEvent.SubmittedTask);
            Assert.True(taskState.CurrentState.Equals(TaskState.TaskSubmitting));
            taskState.MoveNext(TaskEvent.RunningTask);
            Assert.True(taskState.CurrentState.Equals(TaskState.TaskRunning));

            try
            {
                taskState.MoveNext(TaskEvent.SubmittedTask);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvent.ClosedTask);
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
            Assert.True(taskState.CurrentState.Equals(TaskState.TaskNew));
            taskState.MoveNext(TaskEvent.SubmittedTask);
            Assert.True(taskState.CurrentState.Equals(TaskState.TaskSubmitting));
            taskState.MoveNext(TaskEvent.RunningTask);
            Assert.True(taskState.CurrentState.Equals(TaskState.TaskRunning));
            taskState.MoveNext(TaskEvent.FailedTaskEvaluatorError);
            Assert.True(taskState.CurrentState.Equals(TaskState.TaskFailedByEvaluatorFailure));

            try
            {
                taskState.MoveNext(TaskEvent.RunningTask);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvent.ClosedTask);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvent.CompletedTask);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvent.SubmittedTask);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvent.WaitingTaskToClose);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvent.FailedTaskAppError);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvent.FailedTaskCommunicationError);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }

            try
            {
                taskState.MoveNext(TaskEvent.FailedTaskSystemError);
            }
            catch (Exception e)
            {
                Assert.True(e.Message.Contains("Invalid transition:"));
            }
        }
    }
}
