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
    /// TaskSuspendHandler causes a FailedEvaluator event in the Driver.
    /// </summary>
    [Collection("FunctionalTests")]
    public sealed class TaskSuspendExceptionTest : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TaskSuspendExceptionTest));

        private const string TaskSuspendExceptionMessage = "TaskSuspendExceptionMessage";
        private const string InitialTaskPreWaitMessage = "InitialTaskPreWaitMessage";
        private const string InitialTaskPostWaitMessage = "InitialTaskPostWaitMessage";
        private const string FailedEvaluatorReceived = "FailedEvaluatorReceived";
        private const string TaskSuspensionMessage = "TaskSuspensionMessage";

        /// <summary>
        /// This test validates that an Exception in the TaskSuspendHandler causes a FailedEvaluator event.
        /// </summary>
        [Fact]
        public void TestSuspendTaskWithExceptionOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<TaskSuspendExceptionTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<TaskSuspendExceptionTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<TaskSuspendExceptionTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskRunning, GenericType<TaskSuspendExceptionTestDriver>.Class)
                .Build(), typeof(TaskSuspendExceptionTestDriver), 1, "testSuspendTaskWithExceptionOnLocalRuntime", "local", testFolder);

            var driverMessages = new List<string>
            {
                TaskSuspensionMessage,
                FailedEvaluatorReceived
            };

            ValidateMessagesSuccessfullyLoggedForDriver(driverMessages, testFolder, 1);
            ValidateMessageSuccessfullyLogged(driverMessages, "driver", DriverStdout, testFolder, 1);

            var evaluatorMessages = new List<string> { InitialTaskPreWaitMessage };
            ValidateMessageSuccessfullyLogged(evaluatorMessages, "Node-*", EvaluatorStdout, testFolder, 1);
            CleanUp(testFolder);
        }

        private sealed class TaskSuspendExceptionTestDriver :
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<IRunningTask>,
            IObserver<IFailedEvaluator>
        {
            private static readonly string TaskId = "TaskId";

            private readonly IEvaluatorRequestor _requestor;

            [Inject]
            private TaskSuspendExceptionTestDriver(IEvaluatorRequestor requestor)
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
                        .Set(TaskConfiguration.Task, GenericType<TaskSuspendExceptionTask>.Class)
                        .Set(TaskConfiguration.OnSuspend, GenericType<TaskSuspendHandlerWithException>.Class)
                        .Build());
            }

            public void OnNext(IRunningTask value)
            {
                if (value.Id == TaskId)
                {
                    Logger.Log(Level.Info, TaskSuspensionMessage);
                    value.Suspend();
                }
            }

            public void OnNext(IFailedEvaluator value)
            {
                Assert.True(value.FailedTask.IsPresent());
                Assert.Equal(TaskId, value.FailedTask.Value.Id);
                Assert.True(value.EvaluatorException.InnerException is TestSerializableException);
                Assert.Equal(TaskSuspendExceptionMessage, value.EvaluatorException.InnerException.Message);
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

        private sealed class TaskSuspendExceptionTask : WaitingTask
        {
            [Inject]
            private TaskSuspendExceptionTask(EventMonitor monitor)
                : base(monitor, InitialTaskPreWaitMessage, InitialTaskPostWaitMessage)
            {
            }
        }

        private sealed class TaskSuspendHandlerWithException : ExceptionThrowingHandler<ISuspendEvent>
        {
            [Inject]
            private TaskSuspendHandlerWithException(EventMonitor monitor)
                : base(
                    new TestSerializableException(TaskSuspendExceptionMessage),
                    close => { monitor.Signal(); })
            {
            }
        }
    }
}