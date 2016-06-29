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
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Tests.Functional.Common.Task;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Failure.User
{
    [Collection("FunctionalTests")]
    public sealed class ServiceConstructorExceptionTest : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ServiceConstructorExceptionTest));
        private static readonly string FailedEvaluatorReceived = "FailedEvaluatorReceived";
        private static readonly string ActiveContextReceived = "ActiveContextReceived";
        private static readonly string FailedContextReceived = "FailedContextReceived";
        private static readonly string RunningTaskReceived = "RunningTaskReceived";
        private static readonly string CompletedTaskReceived = "CompletedTaskReceived";
        private static readonly string Context0 = "Context0";
        private static readonly string Context1 = "Context1";
        private static readonly string Context2 = "Context2";
        private static readonly string TaskId = "TaskId";
        private static readonly string TaskRunningMessage = "TaskRunningMessage";

        /// <summary>
        /// This test tests that an Exception in the constructor of a Service object triggers a FailedContext event
        /// on a non-root Context. Also tests that an Exception in the constructor of a Service object triggers a FailedEvaluator
        /// event on a root Context. Upon failing the non-root context, we submit a Task to the failed context's parent
        /// to verify that a Task can still be run on the parent context.
        /// </summary>
        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", 
            "Test Exception in the constructor of a Service object and validate that we receive either a FailedContext event " +
            "in the case of non-root contexts, or a FailedEvaluator event in the case of the root context.")]
        public void TestServiceConstructorExceptionOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(
                DriverConfiguration.ConfigurationModule
                    .Set(DriverConfiguration.OnDriverStarted, GenericType<ServiceConstructorExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<ServiceConstructorExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<ServiceConstructorExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnContextActive, GenericType<ServiceConstructorExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnContextFailed, GenericType<ServiceConstructorExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnTaskRunning, GenericType<ServiceConstructorExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnTaskCompleted, GenericType<ServiceConstructorExceptionDriver>.Class)
                    .Build(),
                typeof(ServiceConstructorExceptionDriver), 1, "serviceConstructorExceptionTest", "local", testFolder);

            ValidateSuccessForLocalRuntime(numberOfContextsToClose: 1, numberOfTasksToFail: 0, numberOfEvaluatorsToFail: 1, testFolder: testFolder);
            ValidateMessagesSuccessfullyLoggedForDriver(
                new[] { FailedEvaluatorReceived, ActiveContextReceived, RunningTaskReceived, CompletedTaskReceived }, testFolder, 1);
            CleanUp(testFolder);
        }

        private sealed class ServiceConstructorExceptionDriver : 
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<IActiveContext>,
            IObserver<IFailedContext>,
            IObserver<IFailedEvaluator>,
            IObserver<IRunningTask>,
            IObserver<ICompletedTask>
        {
            private readonly object _lock = new object();
            private readonly IEvaluatorRequestor _requestor;
            private bool _shouldFailOnRootContext = true;

            [Inject]
            private ServiceConstructorExceptionDriver(IEvaluatorRequestor requestor)
            {
                _requestor = requestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().SetNumber(2).Build());
            }

            public void OnNext(IFailedEvaluator value)
            {
                // We should expect 0 failed contexts here, since the Evaluator fails
                // to instantiate the RootContext.
                Assert.Equal(0, value.FailedContexts.Count);
                Logger.Log(Level.Info, FailedEvaluatorReceived);
            }

            /// <summary>
            /// Submits two contexts, one that fails the Evaluator and another that allows for
            /// Context Stacking.
            /// </summary>
            public void OnNext(IAllocatedEvaluator value)
            {
                lock (_lock)
                {
                    if (_shouldFailOnRootContext)
                    {
                        // Failing config.
                        var ctxConf = ContextConfiguration.ConfigurationModule
                            .Set(ContextConfiguration.Identifier, Context0)
                            .Build();

                        var serviceConf = ServiceConfiguration.ConfigurationModule
                            .Set(ServiceConfiguration.Services, GenericType<ServiceConstructorExceptionService>.Class)
                            .Build();

                        value.SubmitContextAndService(ctxConf, serviceConf);
                        _shouldFailOnRootContext = false;
                    }
                    else
                    {
                        // Context stacking config.
                        value.SubmitContext(
                            ContextConfiguration.ConfigurationModule
                                .Set(ContextConfiguration.Identifier, Context1)
                                .Build());
                    }
                }
            }

            /// <summary>
            /// Submits the failing Context config on the non-failing Context.
            /// </summary>
            public void OnNext(IActiveContext value)
            {
                lock (_lock)
                {
                    Logger.Log(Level.Info, ActiveContextReceived);
                    Assert.False(_shouldFailOnRootContext);

                    var ctxConf = ContextConfiguration.ConfigurationModule
                        .Set(ContextConfiguration.Identifier, Context2)
                        .Build();

                    var serviceConf = ServiceConfiguration.ConfigurationModule
                        .Set(ServiceConfiguration.Services, GenericType<ServiceConstructorExceptionService>.Class)
                        .Build();

                    value.SubmitContextAndService(ctxConf, serviceConf);
                }
            }

            /// <summary>
            /// Only Context2 is expected as the FailedContext.
            /// Context0 should trigger a FailedEvaluator directly.
            /// Submit a Task on to the parent of Context2 (Context1) and make sure 
            /// that it runs to completion.
            /// </summary>
            public void OnNext(IFailedContext value)
            {
                Assert.Equal(Context2, value.Id);
                Assert.True(value.ParentContext.IsPresent(), "Expected " + Context2 + " to have parent of " + Context1);

                if (value.ParentContext.IsPresent())
                {
                    // Submit Task on the good Context, i.e. the FailedContext's parent.
                    Assert.Equal(Context1, value.ParentContext.Value.Id);
                    value.ParentContext.Value.SubmitTask(
                        TaskConfiguration.ConfigurationModule
                            .Set(TaskConfiguration.Identifier, TaskId)
                            .Set(TaskConfiguration.Task, GenericType<ServiceConstructorExceptionTestTask>.Class)
                            .Build());
                }

                Logger.Log(Level.Info, FailedContextReceived);
            }

            public void OnNext(IRunningTask value)
            {
                Assert.Equal(TaskId, value.Id);
                Assert.Equal(Context1, value.ActiveContext.Id);
                Logger.Log(Level.Info, RunningTaskReceived);
            }

            public void OnNext(ICompletedTask value)
            {
                Assert.Equal(TaskId, value.Id);
                Logger.Log(Level.Info, CompletedTaskReceived);
                value.ActiveContext.Dispose();
            }

            public void OnError(Exception error)
            {
            }

            public void OnCompleted()
            {
            }
        }

        /// <summary>
        /// A test service class that is not injectable.
        /// </summary>
        private sealed class ServiceConstructorExceptionService
        {
            /// <summary>
            /// Not injectable on purpose to make Service injection fail.
            /// </summary>
            private ServiceConstructorExceptionService()
            {
            }
        }

        /// <summary>
        /// A Test Task class that simply logs a Task running message.
        /// </summary>
        private sealed class ServiceConstructorExceptionTestTask : LoggingTask
        {
            [Inject]
            private ServiceConstructorExceptionTestTask() 
                : base(TaskRunningMessage)
            {
            }
        }
    }
}