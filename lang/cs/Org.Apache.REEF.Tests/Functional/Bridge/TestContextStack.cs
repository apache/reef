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

        public TestContextStack()
        {
            Init();
        }

        /// <summary>
        /// Does a simple test of whether a context can be submitted on top of another context.
        /// </summary>
        [Fact]
        public void TestContextStackingOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            CleanUp(testFolder);
            TestRun(DriverConfigurations(), typeof(ContextStackHandlers), 1, "testContextStack", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, testFolder: testFolder);
            ValidateMessageSuccessfullyLogged(TaskValidationMessage, testFolder);
            ValidateMessageSuccessfullyLogged(ClosedContextValidationMessage, testFolder);
            CleanUp(testFolder);
        }

        public IConfiguration DriverConfigurations()
        {
            var helloDriverConfiguration = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<ContextStackHandlers>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<ContextStackHandlers>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<ContextStackHandlers>.Class)
                .Set(DriverConfiguration.OnTaskMessage, GenericType<HelloTaskMessageHandler>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<ContextStackHandlers>.Class)
                .Set(DriverConfiguration.OnContextClosed, GenericType<ContextStackHandlers>.Class)
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(helloDriverConfiguration).Build();
        }

        private sealed class ContextStackHandlers : 
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<IActiveContext>,
            IObserver<ICompletedTask>,
            IObserver<IClosedContext>
        {
            private readonly IEvaluatorRequestor _requestor;
            private IAllocatedEvaluator _evaluator;

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
                value.SubmitContext(Common.Context.ContextConfiguration.ConfigurationModule
                    .Set(Common.Context.ContextConfiguration.Identifier, ContextOneId)
                    .Build());
                _evaluator = value;
            }

            public void OnNext(IActiveContext value)
            {
                Logger.Log(Level.Verbose, "ContextId: " + value.Id);
                switch (value.Id)
                {
                    case ContextOneId:
                        var contextConfig =
                            Common.Context.ContextConfiguration.ConfigurationModule.Set(
                                Common.Context.ContextConfiguration.Identifier, ContextTwoId)
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

            public void OnNext(ICompletedTask value)
            {
                Logger.Log(Level.Info, TaskValidationMessage);
                value.ActiveContext.Dispose();
            }

            public void OnNext(IClosedContext value)
            {
                // TODO[JIRA REEF-762]: Inspect closing order of contexts.
                Logger.Log(Level.Info, ClosedContextValidationMessage);

                // TODO[JIRA REEF-762]: Remove disposal of Evaluator, since it should naturally be closed if no contexts.
                _evaluator.Dispose();
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

        private interface IInjectableInterface
        {
        }

        private sealed class InjectableInterfaceImpl : IInjectableInterface
        {
            [Inject]
            private InjectableInterfaceImpl()
            {
            }
        }
    }
}