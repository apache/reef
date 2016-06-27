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
using System.Text;
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
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Failure.User
{
    [Collection("FunctionalTests")]
    public sealed class ReceiveTaskMessageExceptionTest : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ReceiveTaskMessageExceptionTest));

        private static readonly string TaskId = "TaskId";
        private static readonly string ExpectedExceptionMessage = "ExpectedExceptionMessage";
        private static readonly string ReceivedFailedEvaluator = "ReceivedFailedEvaluator";

        /// <summary>
        /// This test validates that a failure in the IDriverMessageHandler results in a FailedEvaluator.
        /// </summary>
        [Fact]
        public void TestReceiveTaskMessageException()
        {
            string testFolder = DefaultRuntimeFolder + TestId;

            TestRun(DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<TestReceiveTaskMessageExceptionDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<TestReceiveTaskMessageExceptionDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<TestReceiveTaskMessageExceptionDriver>.Class)
                .Set(DriverConfiguration.OnTaskRunning, GenericType<TestReceiveTaskMessageExceptionDriver>.Class)
                .Build(),
                typeof(TestReceiveTaskMessageExceptionDriver), 1, "ReceiveTaskMessageExceptionTest", "local", testFolder);

            ValidateSuccessForLocalRuntime(0, 0, 1, testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(ReceivedFailedEvaluator, testFolder);
            CleanUp(testFolder);
        }

        private sealed class TestReceiveTaskMessageExceptionDriver :
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<IFailedEvaluator>,
            IObserver<IRunningTask>
        {
            private readonly IEvaluatorRequestor _requestor;

            [Inject]
            private TestReceiveTaskMessageExceptionDriver(IEvaluatorRequestor requestor)
            {
                _requestor = requestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().Build());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                value.SubmitTask(
                    TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, TaskId)
                        .Set(TaskConfiguration.Task, GenericType<TestTask>.Class)
                        .Set(TaskConfiguration.OnMessage, GenericType<ReceiveTaskMessageExceptionHandler>.Class)
                        .Build());
            }

            /// <summary>
            /// Throwing an Exception in a Driver message handler will result in a Failed Evaluator.
            /// We check for the Task ID and Exception type here.
            /// </summary>
            public void OnNext(IFailedEvaluator value)
            {
                Assert.Equal(1, value.FailedContexts.Count);
                Assert.True(value.FailedTask.IsPresent());
                Assert.Equal(TaskId, value.FailedTask.Value.Id);
                Assert.NotNull(value.EvaluatorException.InnerException);
                Assert.True(value.EvaluatorException.InnerException is TestSerializableException);
                Assert.Equal(ExpectedExceptionMessage, value.EvaluatorException.InnerException.Message);

                Logger.Log(Level.Info, ReceivedFailedEvaluator);
            }

            public void OnNext(IRunningTask value)
            {
                value.Send(Encoding.UTF8.GetBytes("Hello from Driver!"));
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

        private sealed class ReceiveTaskMessageExceptionHandler : IDriverMessageHandler
        {
            [Inject]
            private ReceiveTaskMessageExceptionHandler()
            {
            }

            public void Handle(IDriverMessage message)
            {
                throw new TestSerializableException(ExpectedExceptionMessage);
            }
        }

        private sealed class TestTask : WaitingTask
        {
            [Inject]
            private TestTask(EventHandle eventHandle) : base(eventHandle)
            {
            }
        }
    }
}