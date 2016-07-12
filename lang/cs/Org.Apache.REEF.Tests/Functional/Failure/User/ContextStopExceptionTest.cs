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
using Org.Apache.REEF.Tests.Functional.Common.Task;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Failure.User
{
    /// <summary>
    /// This class contains a test that tests whether throwing an Exception in ContextStopHandler behaves correctly.
    /// </summary>
    [Collection("FunctionalTests")]
    public sealed class ContextStopExceptionTest : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ContextStopExceptionTest));

        private static readonly string ExpectedException = "ExpectedException";
        private static readonly string FailEvaluatorContextId = "FailEvaluatorContextId";
        private static readonly string ContextId0 = "ContextId0";
        private static readonly string ContextId1 = "ContextId1";
        private static readonly string TaskId = "TaskId";
        private static readonly string CompletedTaskReceived = "CompletedTaskReceived";
        private static readonly string FailedContextReceived = "FailedContextReceived";
        private static readonly string FailedEvaluatorReceived = "FailedEvaluatorReceived";

        /// <summary>
        /// This test tests whether throwing an Exception in ContextStopHandler behaves correctly.
        /// The test requests two Evaluators, the first Evaluator will only have a Root Context on which
        /// will throw an Exception upon calling ActiveContext.Dispose. The second Evaluator will have two
        /// stacked Contexts - ContextId1 will be stacked on top of ContextId0. ContextId1 will contain the
        /// failing ContextStopHandler, in which the Evaluator will pop the Context and submit a Task to ContextId0,
        /// verify that the Task completes successfully, and verify that the Context is disposed without an Exception.
        /// </summary>
        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test throwing an Exception in ContextStopHandler should cause the Driver to receive a ContextFailed event." +
                              "In the case of the Root Context, the Driver should receive a FailedEvaluator event.")]
        public void TestContextStopException()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(
                DriverConfiguration.ConfigurationModule
                    .Set(DriverConfiguration.OnDriverStarted, GenericType<ContextStopExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<ContextStopExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<ContextStopExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnContextActive, GenericType<ContextStopExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnContextFailed, GenericType<ContextStopExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnTaskCompleted, GenericType<ContextStopExceptionDriver>.Class)
                    .Build(),
                typeof(ContextStopExceptionDriver), 1, "ContextStopExceptionTest", "local", testFolder);

            ValidateSuccessForLocalRuntime(numberOfContextsToClose: 3, numberOfTasksToFail: 0, numberOfEvaluatorsToFail: 1, testFolder: testFolder);
            var driverMessages = new[]
            {
                CompletedTaskReceived,
                FailedContextReceived,
                FailedEvaluatorReceived
            };

            ValidateMessagesSuccessfullyLoggedForDriver(driverMessages, testFolder);
            CleanUp(testFolder);
        }

        private sealed class ContextStopExceptionDriver : 
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
            private ContextStopExceptionDriver(IEvaluatorRequestor requestor)
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
                        // This context should fail the Evaluator upon calling ActiveContext.Dispose().
                        value.SubmitContext(
                            GetContextStopExceptionContextConfiguration(FailEvaluatorContextId));
                        _shouldSubmitFailEvaluatorContext = false;
                    }
                    else
                    {
                        // This is the Context that will be stacked upon by ContextId1.
                        value.SubmitContext(
                            ContextConfiguration.ConfigurationModule
                                .Set(ContextConfiguration.Identifier, ContextId0)
                                .Build());
                    }
                }
            }

            /// <summary>
            /// This will be ContextId1.
            /// It will submit a Task to the parent of Context with ContextId1.
            /// </summary>
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
                        .Set(TaskConfiguration.Task, GenericType<NullTask>.Class)
                        .Build());
            }

            /// <summary>
            /// This will be a FailedEvaluator generated by the Context with ID as FailEvaluatorContextId.
            /// </summary>
            public void OnNext(IFailedEvaluator value)
            {
                Assert.Equal(1, value.FailedContexts.Count);
                Assert.Equal(FailEvaluatorContextId, value.FailedContexts.First().Id);
                Assert.NotNull(value.EvaluatorException.InnerException);
                Assert.True(value.EvaluatorException.InnerException is TestSerializableException);
                Assert.Equal(ExpectedException, value.EvaluatorException.InnerException.Message);
                Logger.Log(Level.Info, FailedEvaluatorReceived);
            }

            public void OnNext(IActiveContext value)
            {
                if (value.Id.Equals(FailEvaluatorContextId))
                {
                    // Close context and trigger failure immediately.
                    value.Dispose();
                }
                else
                {
                    if (value.Id.Equals(ContextId0))
                    {
                        // Stack Context with ContextId1 on top of Context with ContextId0.
                        value.SubmitContext(GetContextStopExceptionContextConfiguration(ContextId1));
                    }
                    else
                    {
                        // Verify the stacked Context and close it.
                        Assert.Equal(ContextId1, value.Id);
                        value.Dispose();
                    }
                }
            }

            public void OnNext(ICompletedTask value)
            {
                // Verify the completion of Task on Context with ContextId0.
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

            private static IConfiguration GetContextStopExceptionContextConfiguration(string contextId)
            {
                return ContextConfiguration.ConfigurationModule
                    .Set(ContextConfiguration.Identifier, contextId)
                    .Set(ContextConfiguration.OnContextStop, GenericType<ContextStopExceptionHandler>.Class)
                    .Build();
            }
        }

        /// <summary>
        /// A ContextStopHandler that throws a Serializable Exception.
        /// </summary>
        private sealed class ContextStopExceptionHandler : IObserver<IContextStop>
        {
            [Inject]
            private ContextStopExceptionHandler()
            {
            }

            public void OnNext(IContextStop value)
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
