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
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Tests.Functional.Bridge.Exceptions;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Failure.User
{
    [Collection("FunctionalTests")]
    public sealed class ContextStartExceptionTest : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ContextStartExceptionTest));

        private static readonly string ExpectedException = "ExpectedException";
        private static readonly string FailEvaluatorContextId = "FailEvaluatorContextId";
        private static readonly string ContextId0 = "ContextId0";
        private static readonly string ContextId1 = "ContextId1";
        private static readonly string TaskId = "TaskId";
        private static readonly string CompletedTaskReceived = "CompletedTaskReceived";
        private static readonly string FailedContextReceived = "FailedContextReceived";
        private static readonly string FailedEvaluatorReceived = "FailedEvaluatorReceived";

        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test throwing an Exception in ContextStartHandler should cause the Driver to receive a ContextFailed event." +
                              "In the case of the Root Context, the Driver should receive a FailedEvaluator event.")]
        public void TestContextStartException()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(
                DriverConfiguration.ConfigurationModule
                    .Set(DriverConfiguration.OnDriverStarted, GenericType<ContextStartExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<ContextStartExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<ContextStartExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnContextActive, GenericType<ContextStartExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnContextFailed, GenericType<ContextStartExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnTaskCompleted, GenericType<ContextStartExceptionDriver>.Class)
                    .Build(),
                typeof(ContextStartExceptionDriver), 1, "ContextStartExceptionTest", "local", testFolder);

            ValidateSuccessForLocalRuntime(numberOfContextsToClose: 1, numberOfTasksToFail: 0, numberOfEvaluatorsToFail: 1, testFolder: testFolder);
            var driverMessages = new[]
            {
                CompletedTaskReceived,
                FailedContextReceived,
                FailedEvaluatorReceived
            };

            ValidateMessagesSuccessfullyLoggedForDriver(driverMessages, testFolder);
            CleanUp(testFolder);
        }

        private sealed class ContextStartExceptionDriver : 
            IObserver<IDriverStarted>, 
            IObserver<IAllocatedEvaluator>,
            IObserver<IActiveContext>,
            IObserver<IFailedContext>,
            IObserver<IFailedEvaluator>,
            IObserver<ICompletedTask>
        {
            private readonly IEvaluatorRequestor _requestor;
            private readonly object _lock = new object();
            private bool _shouldSubmitFailEvaluatorContext = true;

            [Inject]
            private ContextStartExceptionDriver(IEvaluatorRequestor requestor)
            {
                _requestor = requestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().SetNumber(2).Build());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                lock (_lock)
                {
                    if (_shouldSubmitFailEvaluatorContext)
                    {
                        value.SubmitContext(
                            GetContextStartExceptionContextConfiguration(FailEvaluatorContextId));
                        _shouldSubmitFailEvaluatorContext = false;
                    }
                    else
                    {
                        value.SubmitContext(
                            ContextConfiguration.ConfigurationModule
                                .Set(ContextConfiguration.Identifier, ContextId0)
                                .Build());
                    }
                }
            }

            public void OnNext(IFailedContext value)
            {
                Assert.Equal(ContextId1, value.Id);
                Assert.True(value.ParentContext.IsPresent());
                Assert.Equal(ContextId0, value.ParentContext.Value.Id);

                // TODO[JIRA REEF-1468]: Validate that Exception is properly serialized.
                Logger.Log(Level.Info, FailedContextReceived);

                value.ParentContext.Value.SubmitTask(
                    TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, TaskId)
                        .Set(TaskConfiguration.Task, GenericType<ContextStartExceptionTask>.Class)
                        .Build());
            }

            public void OnNext(IFailedEvaluator value)
            {
                // We should not have any failed contexts since the context has never become active.
                Assert.Equal(0, value.FailedContexts.Count);
                Assert.NotNull(value.EvaluatorException.InnerException);
                Assert.True(value.EvaluatorException.InnerException is TestSerializableException);
                Assert.Equal(ExpectedException, value.EvaluatorException.InnerException.Message);
                Logger.Log(Level.Info, FailedEvaluatorReceived);
            }

            public void OnNext(IActiveContext value)
            {
                Assert.Equal(ContextId0, value.Id);
                value.SubmitContext(GetContextStartExceptionContextConfiguration(ContextId1));
            }

            public void OnNext(ICompletedTask value)
            {
                Assert.Equal(TaskId, value.Id);
                Assert.Equal(ContextId0, value.ActiveContext.Id);
                Logger.Log(Level.Info, CompletedTaskReceived);
                value.ActiveContext.Dispose();
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }

            private static IConfiguration GetContextStartExceptionContextConfiguration(string contextId)
            {
                return ContextConfiguration.ConfigurationModule
                    .Set(ContextConfiguration.Identifier, contextId)
                    .Set(ContextConfiguration.OnContextStart, GenericType<ContextStartExceptionHandler>.Class)
                    .Build();
            }
        }

        private sealed class ContextStartExceptionTask : ITask
        {
            [Inject]
            private ContextStartExceptionTask()
            {
            }

            public byte[] Call(byte[] memento)
            {
                return null;
            }

            public void Dispose()
            {
            }
        }

        private sealed class ContextStartExceptionHandler : IObserver<IContextStart>
        {
            [Inject]
            private ContextStartExceptionHandler()
            {
            }

            public void OnNext(IContextStart value)
            {
                throw new TestSerializableException(ExpectedException);
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
    }
}
