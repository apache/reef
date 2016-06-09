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
using System.Collections.Generic;
using System.Runtime.Serialization;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Tests.Functional.Bridge.Exceptions;
using Org.Apache.REEF.Tests.Functional.Common;
using Org.Apache.REEF.Tests.Functional.Common.Task;
using Org.Apache.REEF.Tests.Functional.Common.Task.Handlers;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Failure.User
{
    /// <summary>
    /// This test class contains a test that validates that an Exception in the 
    /// TaskCloseHandler causes a FailedTask event in the Driver.
    /// </summary>
    [Collection("FunctionalTests")]
    public sealed class TaskCloseExceptionTest : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TaskCloseExceptionTest));

        private const string TaskCloseExceptionMessage = "TaskCloseExceptionMessage";
        private const string FailedEvaluatorReceived = "FailedEvaluatorReceived";

        /// <summary>
        /// This test validates that an Exception in the TaskCloseHandler causes a FailedTask
        /// event in the Driver, and that a new Task can be submitted on the original Context.
        /// </summary>
        [Fact]
        public void TestCloseTaskWithExceptionOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<TaskCloseExceptionTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<TaskCloseExceptionTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<TaskCloseExceptionTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskRunning, GenericType<TaskCloseExceptionTestDriver>.Class)
                .Build(), typeof(TaskCloseExceptionTestDriver), 1, "testCloseTaskWithExceptionOnLocalRuntime", "local", testFolder);

            var driverMessages = new List<string>
            {
                FailedEvaluatorReceived
            };

            ValidateSuccessForLocalRuntime(numberOfContextsToClose: 0, numberOfEvaluatorsToFail: 1, testFolder: testFolder);
            ValidateMessagesSuccessfullyLoggedForDriver(driverMessages, testFolder, 1);
            ValidateMessageSuccessfullyLogged(driverMessages, "driver", DriverStdout, testFolder, 1);
            CleanUp(testFolder);
        }

        private sealed class TaskCloseExceptionTestDriver :
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<IRunningTask>,
            IObserver<IFailedEvaluator>
        {
            private static readonly string TaskId = "TaskId";

            private readonly IEvaluatorRequestor _requestor;

            [Inject]
            private TaskCloseExceptionTestDriver(IEvaluatorRequestor requestor)
            {
                _requestor = requestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().Build());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                // submit the first Task.
                value.SubmitTask(TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, TaskId)
                        .Set(TaskConfiguration.Task, GenericType<TaskCloseExceptionTask>.Class)
                        .Set(TaskConfiguration.OnClose, GenericType<TaskCloseHandlerWithException>.Class)
                        .Build());
            }

            public void OnNext(IRunningTask value)
            {
                if (value.Id == TaskId)
                {
                    value.Dispose();
                }
            }

            public void OnNext(IFailedEvaluator value)
            {
                Assert.True(value.FailedTask.IsPresent());
                var failedTask = value.FailedTask.Value;

                Assert.Equal(TaskId, failedTask.Id);

                // Check that Exceptions are deserialized correctly.
                var ex = value.EvaluatorException.InnerException;
                if (ex == null)
                {
                    throw new Exception("Exception was not expected to be null.");
                }

                var taskCloseEx = ex as TestSerializableException;

                if (taskCloseEx == null)
                {
                    throw new Exception("Expected Exception to be of type TaskCloseExceptionTestException, but instead got type " + ex.GetType().Name);
                }

                if (taskCloseEx.Message != TaskCloseExceptionMessage)
                {
                    throw new Exception(
                        "Expected message to be " + TaskCloseExceptionMessage + " but instead got " + taskCloseEx.Message + ".");
                }

                Logger.Log(Level.Info, FailedEvaluatorReceived);
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }
        }

        private sealed class TaskCloseExceptionTask : WaitingTask
        {
            [Inject]
            private TaskCloseExceptionTask(EventMonitor monitor) : base(monitor)
            {
            }
        }

        private sealed class TaskCloseHandlerWithException : ExceptionThrowingHandler<ICloseEvent>
        {
            [Inject]
            private TaskCloseHandlerWithException(EventMonitor monitor) : base(
                new TestSerializableException(TaskCloseExceptionMessage),
                close => { monitor.Signal(); })
            {
            }
        }
    }
}