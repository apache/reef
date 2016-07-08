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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Tests.Functional.Bridge.Exceptions;
using Org.Apache.REEF.Tests.Functional.Common;
using Org.Apache.REEF.Tests.Functional.Common.Task;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Failure.User
{
    /// <summary>
    /// This class contains a test that tests the behavior upon throwing an Exception when
    /// sending a Context Message from the Evaluator's IContextMessageSource.
    /// </summary>
    [Collection("FunctionalTests")]
    public sealed class SendTaskMessageExceptionTest : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(SendTaskMessageExceptionTest));

        private static readonly string TaskId = "TaskId";
        private static readonly string ExpectedExceptionMessage = "ExpectedExceptionMessage";
        private static readonly string ReceivedFailedEvaluator = "ReceivedFailedEvaluator";

        /// <summary>
        /// This test validates that a failure in the ITaskMessageSource results in a FailedEvaluator.
        /// </summary>
        [Fact]
        public void TestSendTaskMessageException()
        {
            string testFolder = DefaultRuntimeFolder + TestId;

            TestRun(DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<TestSendTaskMessageExceptionDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<TestSendTaskMessageExceptionDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<TestSendTaskMessageExceptionDriver>.Class)
                .Set(DriverConfiguration.OnContextFailed, GenericType<TestSendTaskMessageExceptionDriver>.Class)
                .Build(),
                typeof(TestSendTaskMessageExceptionDriver),
                1,
                "SendTaskMessageExceptionTest",
                "local",
                testFolder);

            ValidateSuccessForLocalRuntime(0, 0, 1, testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(ReceivedFailedEvaluator, testFolder);
            CleanUp(testFolder);
        }

        private sealed class TestSendTaskMessageExceptionDriver :
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<IFailedContext>,
            IObserver<IFailedEvaluator>
        {
            private readonly IEvaluatorRequestor _requestor;

            [Inject]
            private TestSendTaskMessageExceptionDriver(IEvaluatorRequestor requestor)
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
                        .Set(TaskConfiguration.OnSendMessage, GenericType<SendTaskMessageExceptionHandler>.Class)
                        .Build());
            }

            /// <summary>
            /// Throwing an Exception in a task message handler will result in a Failed Evaluator.
            /// </summary>
            public void OnNext(IFailedEvaluator value)
            {
                Assert.Equal(1, value.FailedContexts.Count);
                Assert.NotNull(value.EvaluatorException.InnerException);
                Assert.True(value.EvaluatorException.InnerException is TestSerializableException,
                    "Unexpected type of evaluator exception: " + value.EvaluatorException.InnerException.GetType());
                Assert.Equal(ExpectedExceptionMessage, value.EvaluatorException.InnerException.Message);

                Logger.Log(Level.Info, ReceivedFailedEvaluator);
            }

            public void OnNext(IFailedContext value)
            {
                throw new Exception("The Driver does not expect a Failed Context message.");
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

        /// <summary>
        /// A Context message source that throws an Exception.
        /// </summary>
        private sealed class SendTaskMessageExceptionHandler : ITaskMessageSource
        {
            private int counter;

            [Inject]
            private SendTaskMessageExceptionHandler()
            {
            }

            public Optional<TaskMessage> Message
            {
                get
                {
                    counter++;
                    if (counter == 2)
                    {
                        throw new TestSerializableException(ExpectedExceptionMessage);
                    }
                    return Optional<TaskMessage>.Empty();
                }
            }
        }

        private sealed class TestTask : WaitingTask
        {
            [Inject]
            private TestTask(EventMonitor eventMonitor) : base(eventMonitor, "WaitingTask started")
            {
            }
        }
    }
}