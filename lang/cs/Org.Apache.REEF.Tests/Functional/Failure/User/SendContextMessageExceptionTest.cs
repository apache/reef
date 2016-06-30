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
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Tests.Functional.Bridge.Exceptions;
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
    public sealed class SendContextMessageExceptionTest : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(SendContextMessageExceptionTest));

        private static readonly string ContextId = "ContextId";
        private static readonly string ExpectedExceptionMessage = "ExpectedExceptionMessage";
        private static readonly string ReceivedFailedEvaluator = "ReceivedFailedEvaluator";

        /// <summary>
        /// This test validates that a failure in the IContextMessageSource results in a FailedEvaluator.
        /// </summary>
        [Fact]
        public void TestSendContextMessageException()
        {
            string testFolder = DefaultRuntimeFolder + TestId;

            TestRun(DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<TestSendContextMessageExceptionDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<TestSendContextMessageExceptionDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<TestSendContextMessageExceptionDriver>.Class)
                .Set(DriverConfiguration.OnContextFailed, GenericType<TestSendContextMessageExceptionDriver>.Class)
                .Build(),
                typeof(TestSendContextMessageExceptionDriver), 1, "SendContextMessageExceptionTest", "local", testFolder);

            ValidateSuccessForLocalRuntime(0, 0, 1, testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(ReceivedFailedEvaluator, testFolder);
            CleanUp(testFolder);
        }

        private sealed class TestSendContextMessageExceptionDriver :
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<IFailedContext>,
            IObserver<IFailedEvaluator>
        {
            private readonly IEvaluatorRequestor _requestor;

            [Inject]
            private TestSendContextMessageExceptionDriver(IEvaluatorRequestor requestor)
            {
                _requestor = requestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().Build());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                var contextConfig = ContextConfiguration.ConfigurationModule
                    .Set(ContextConfiguration.Identifier, ContextId)
                    .Set(ContextConfiguration.OnSendMessage, GenericType<SendContextMessageExceptionHandler>.Class)
                    .Build();

                value.SubmitContext(contextConfig);
            }

            /// <summary>
            /// Throwing an Exception in a Context message handler will result in a Failed Evaluator.
            /// </summary>
            public void OnNext(IFailedEvaluator value)
            {
                // We will not be expecting any failed contexts here, this is because the Exception
                // is thrown on the heartbeat to the Driver, and will thus fail before the initial heartbeat
                // to the Driver is sent.
                Assert.Equal(0, value.FailedContexts.Count);

                Assert.NotNull(value.EvaluatorException.InnerException);
                Assert.True(value.EvaluatorException.InnerException is TestSerializableException);
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
        private sealed class SendContextMessageExceptionHandler : IContextMessageSource
        {
            [Inject]
            private SendContextMessageExceptionHandler()
            {
            }

            public Optional<ContextMessage> Message
            {
                get { throw new TestSerializableException(ExpectedExceptionMessage); }
            }
        }
    }
}