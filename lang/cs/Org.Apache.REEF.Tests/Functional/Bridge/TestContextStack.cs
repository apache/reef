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
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Examples.AllHandlers;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Bridge
{
    [Collection("FunctionalTests")]
    public sealed class TestContextStack : ReefFunctionalTest
    {
        private const string ContextOneId = "Context1";
        private const string ContextTwoId = "Context2";
        private const string TaskValidationMessage = "TaskValidationMessage";
        private const string ClosedContextValidationMessage = "ClosedContextValidationMessage";

        private static readonly Logger Logger = Logger.GetLogger(typeof(TestContextStack));

        /// <summary>
        /// Does a simple test of whether a context can be submitted on top of another context.
        /// </summary>
        [Fact]
        public void TestContextStackingOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfigurations(GenericType<ActiveContextSubmitContextHandler>.Class),
                typeof(ContextStackHandlers), 1, "testContextStack", "local", testFolder);
            ValidateSuccessForLocalRuntime(2, testFolder: testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(TaskValidationMessage, testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(ClosedContextValidationMessage, testFolder);
            CleanUp(testFolder);
        }

        /// <summary>
        /// Does a simple test of whether a context can be submitted on top of another context 
        /// using SubmitContextAndService.
        /// </summary>
        [Fact]
        public void TestContextStackingWithServiceOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfigurations(GenericType<ActiveContextSubmitContextAndServiceHandler>.Class),
                typeof(ContextStackHandlers), 1, "testContextAndServiceStack", "local", testFolder);
            ValidateSuccessForLocalRuntime(2, testFolder: testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(TaskValidationMessage, testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(ClosedContextValidationMessage, testFolder);
            CleanUp(testFolder);
        }

        public IConfiguration DriverConfigurations<T>(GenericType<T> activeContextHandlerType) where T : IObserver<IActiveContext>
        {
            return TangFactory.GetTang().NewConfigurationBuilder(
                DriverConfiguration.ConfigurationModule
                    .Set(DriverConfiguration.OnDriverStarted, GenericType<ContextStackHandlers>.Class)
                    .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<ContextStackHandlers>.Class)
                    .Set(DriverConfiguration.OnContextActive, activeContextHandlerType)
                    .Set(DriverConfiguration.OnTaskMessage, GenericType<HelloTaskMessageHandler>.Class)
                    .Set(DriverConfiguration.OnTaskCompleted, GenericType<ContextStackHandlers>.Class)
                    .Set(DriverConfiguration.OnContextClosed, GenericType<ContextStackHandlers>.Class)
                    .Build())
                    .Build();
        }

        /// <summary>
        /// ActiveContext Handler that stacks 2 contexts and submits a Task on the second context.
        /// </summary>
        private sealed class ActiveContextSubmitContextHandler : IObserver<IActiveContext>
        {
            [Inject]
            private ActiveContextSubmitContextHandler()
            {
            }

            public void OnNext(IActiveContext value)
            {
                Logger.Log(Level.Verbose, "ContextId: " + value.Id);
                switch (value.Id)
                {
                    case ContextOneId:
                        var contextConfig =
                            REEF.Common.Context.ContextConfiguration.ConfigurationModule.Set(
                                REEF.Common.Context.ContextConfiguration.Identifier, ContextTwoId)
                                .Build();
                        var stackingContextConfig =
                            TangFactory.GetTang()
                                .NewConfigurationBuilder()
                                .BindImplementation(GenericType<IInjectableInterface>.Class,
                                    GenericType<InjectableInterfaceImpl>.Class)
                                    .Build();

                        Assert.False(value.ParentId.IsPresent());

                        value.SubmitContext(Configurations.Merge(stackingContextConfig, contextConfig));
                        break;
                    case ContextTwoId:
                        Assert.True(value.ParentId.IsPresent());
                        Assert.Equal(value.ParentId.Value, ContextOneId);

                        value.SubmitTask(
                            TaskConfiguration.ConfigurationModule.Set(TaskConfiguration.Identifier, "contextStackTestTask")
                                .Set(TaskConfiguration.Task, GenericType<TestContextStackTask>.Class)
                                .Build());
                        break;
                    default:
                        throw new Exception("Unexpected ContextId: " + value.Id);
                }
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
        /// Context Start Handler that invokes Start on the injected TestService.
        /// </summary>
        private sealed class TestContextStackContextStartHandler : IObserver<IContextStart>
        {
            private readonly TestService _service;

            [Inject]
            private TestContextStackContextStartHandler(TestService service)
            {
                _service = service;
            }

            public void OnNext(IContextStart value)
            {
                _service.Start();
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
        /// ActiveContext Handler that stacks 2 contexts.
        /// Primarily used to test out the functionality of SubmitContextAndService. 
        /// The ActiveContext Handler starts the TestService with a ContextStartHandler on the second Context.
        /// </summary>
        private sealed class ActiveContextSubmitContextAndServiceHandler : IObserver<IActiveContext>
        {
            [Inject]
            private ActiveContextSubmitContextAndServiceHandler()
            {
            }

            public void OnNext(IActiveContext value)
            {
                Logger.Log(Level.Verbose, "ContextId: " + value.Id);
                switch (value.Id)
                {
                    case ContextOneId:
                        var contextConfig =
                            REEF.Common.Context.ContextConfiguration.ConfigurationModule
                                .Set(REEF.Common.Context.ContextConfiguration.Identifier, ContextTwoId)
                                .Set(REEF.Common.Context.ContextConfiguration.OnContextStart, GenericType<TestContextStackContextStartHandler>.Class)
                                .Build();

                        var stackingContextConfig =
                            TangFactory.GetTang()
                                .NewConfigurationBuilder()
                                .BindImplementation(GenericType<IInjectableInterface>.Class,
                                    GenericType<InjectableInterfaceImpl>.Class)
                                    .Build();

                        Assert.False(value.ParentId.IsPresent());

                        var stackingContextServiceConfig =
                            ServiceConfiguration.ConfigurationModule
                                .Set(ServiceConfiguration.Services, GenericType<TestService>.Class)
                                .Build();

                        value.SubmitContextAndService(
                            Configurations.Merge(stackingContextConfig, contextConfig), stackingContextServiceConfig);

                        break;
                    case ContextTwoId:
                        Assert.True(value.ParentId.IsPresent());
                        Assert.Equal(value.ParentId.Value, ContextOneId);

                        value.SubmitTask(
                            TaskConfiguration.ConfigurationModule.Set(TaskConfiguration.Identifier, "contextServiceStackTestTask")
                                .Set(TaskConfiguration.Task, GenericType<TestContextAndServiceStackTask>.Class)
                                .Build());
                        break;
                    default:
                        throw new Exception("Unexpected ContextId: " + value.Id);
                }
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
        /// A simple Service class.
        /// </summary>
        private sealed class TestService
        {
            [Inject]
            private TestService()
            {
                Started = false;
            }

            public void Start()
            {
                Started = true;
            }

            /// <summary>
            /// Returns whether the Start function has been called or not.
            /// </summary>
            public bool Started { get; private set; }
        }

        /// <summary>
        /// Basic handlers used to verify that Contexts are indeed stacked.
        /// </summary>
        private sealed class ContextStackHandlers : 
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<ICompletedTask>,
            IObserver<IClosedContext>
        {
            private readonly IEvaluatorRequestor _requestor;

            [Inject]
            private ContextStackHandlers(IEvaluatorRequestor evaluatorRequestor)
            {
                _requestor = evaluatorRequestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().Build());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                value.SubmitContext(REEF.Common.Context.ContextConfiguration.ConfigurationModule
                    .Set(REEF.Common.Context.ContextConfiguration.Identifier, ContextOneId)
                    .Build());
            }

            public void OnNext(ICompletedTask value)
            {
                Logger.Log(Level.Info, TaskValidationMessage);
                value.ActiveContext.Dispose();
            }

            public void OnNext(IClosedContext value)
            {
                Logger.Log(Level.Info, ClosedContextValidationMessage);

                Assert.Equal(value.Id, ContextTwoId);
                Assert.True(value.ParentId.IsPresent());
                Assert.Equal(value.ParentId.Value, ContextOneId);
                Assert.Equal(value.ParentContext.Id, ContextOneId);

                value.ParentContext.Dispose();
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
        /// A Task to ensure that an object configured in the second context configuration 
        /// is properly injected.
        /// </summary>
        private sealed class TestContextStackTask : ITask
        {
            [Inject]
            private TestContextStackTask(IInjectableInterface injectableInterface)
            {
                Assert.NotNull(injectableInterface);
                Assert.True(injectableInterface is InjectableInterfaceImpl);
            }

            public void Dispose()
            {
            }

            public byte[] Call(byte[] memento)
            {
                return null;
            }
        }

        /// <summary>
        /// A Task to ensure that an object configured in the second context configuration 
        /// is properly injected.
        /// </summary>
        private sealed class TestContextAndServiceStackTask : ITask
        {
            [Inject]
            private TestContextAndServiceStackTask(IInjectableInterface injectableInterface, TestService service)
            {
                Assert.NotNull(injectableInterface);
                Assert.True(injectableInterface is InjectableInterfaceImpl);
                Assert.NotNull(service);
                Assert.True(service.Started);
            }

            public void Dispose()
            {
            }

            public byte[] Call(byte[] memento)
            {
                return null;
            }
        }

        /// <summary>
        /// Empty interface to check whether Context configurations are
        /// set correctly or not on context stacking.
        /// </summary>
        private interface IInjectableInterface
        {
        }

        /// <summary>
        /// An implementation of <see cref="IInjectableInterface"/>.
        /// </summary>
        private sealed class InjectableInterfaceImpl : IInjectableInterface
        {
            [Inject]
            private InjectableInterfaceImpl()
            {
            }
        }
    }
}