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
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Failure.User
{
    [Collection("FunctionalTests")]
    public sealed class ReceiveContextMessageExceptionTest : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ReceiveContextMessageExceptionTest));

        private static readonly string ContextId = "ContextId";
        private static readonly string ExpectedExceptionMessage = "ExpectedExceptionMessage";
        private static readonly string ReceivedFailedEvaluator = "ReceivedFailedEvaluator";

        /// <summary>
        /// This test validates that a failure in the IContextMessageHandler results in a FailedEvaluator.
        /// </summary>
        [Fact]
        public void TestReceiveContextMessageException()
        {
            string testFolder = DefaultRuntimeFolder + TestId;

            TestRun(DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<TestReceiveContextMessageExceptionDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<TestReceiveContextMessageExceptionDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<TestReceiveContextMessageExceptionDriver>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<TestReceiveContextMessageExceptionDriver>.Class)
                .Build(),
                typeof(TestReceiveContextMessageExceptionDriver), 1, "ReceiveContextMessageExceptionTest", "local", testFolder);

            ValidateSuccessForLocalRuntime(0, 0, 1, testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(ReceivedFailedEvaluator, testFolder);
            CleanUp(testFolder);
        }

        private sealed class TestReceiveContextMessageExceptionDriver :
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<IActiveContext>,
            IObserver<IFailedEvaluator>
        {
            private readonly IEvaluatorRequestor _requestor;

            [Inject]
            private TestReceiveContextMessageExceptionDriver(IEvaluatorRequestor requestor)
            {
                _requestor = requestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().Build());
            }

            public void OnNext(IActiveContext value)
            {
                value.SendMessage(Encoding.UTF8.GetBytes("Hello!"));
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                var contextConfig = ContextConfiguration.ConfigurationModule
                    .Set(ContextConfiguration.Identifier, ContextId)
                    .Set(ContextConfiguration.OnMessage, GenericType<ReceiveContextMessageExceptionHandler>.Class)
                    .Build();

                value.SubmitContext(contextConfig);
            }

            /// <summary>
            /// Throwing an Exception in a Context message handler will result in a Failed Evaluator.
            /// We check for the Context ID and Exception type here.
            /// </summary>
            public void OnNext(IFailedEvaluator value)
            {
                Assert.Equal(1, value.FailedContexts.Count);
                Assert.Equal(ContextId, value.FailedContexts.Single().Id);
                Assert.NotNull(value.EvaluatorException.InnerException);
                Assert.True(value.EvaluatorException.InnerException is ReceiveContextMessageExceptionTestException);
                Assert.Equal(ExpectedExceptionMessage, value.EvaluatorException.InnerException.Message);

                Logger.Log(Level.Info, ReceivedFailedEvaluator);
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

        private sealed class ReceiveContextMessageExceptionHandler : IContextMessageHandler
        {
            [Inject]
            private ReceiveContextMessageExceptionHandler()
            {
            }

            public void OnNext(byte[] value)
            {
                throw new ReceiveContextMessageExceptionTestException(ExpectedExceptionMessage);
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

        [Serializable]
        private sealed class ReceiveContextMessageExceptionTestException : Exception
        {
            internal ReceiveContextMessageExceptionTestException(string message) 
                : base(message)
            {
            }

            private ReceiveContextMessageExceptionTestException(SerializationInfo info, StreamingContext ctx)
                : base(info, ctx)
            {
            }
        }
    }
}