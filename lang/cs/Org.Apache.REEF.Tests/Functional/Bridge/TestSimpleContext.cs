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
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Examples.AllHandlers;
using Org.Apache.REEF.Examples.Tasks.HelloTask;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using ContextConfiguration = Org.Apache.REEF.Common.Context.ContextConfiguration;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Bridge
{
    [Collection("FunctionalTests")]
    public sealed class TestSimpleContext : ReefFunctionalTest
    {
        private const string ContextId = "ContextId";
        private const string ValidationMessage = "ValidationMessage";

        private static readonly Logger Logger = Logger.GetLogger(typeof(TestSimpleContext));

        public TestSimpleContext()
        {
            Init();
        }

        /// <summary>
        /// Does a simple test of context submission.
        /// </summary>
        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test Context ID submission on local runtime")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestSimpleContextOnLocalRuntime()
        {
            TestContextOnLocalRuntime(ContextDriverConfiguration());
        }

        /// <summary>
        /// Does a simple test of context submission with deprecated configurations.
        /// </summary>
        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test deprecated Context ID submission on local runtime")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestDeprecatedContextOnLocalRuntime()
        {
            TestContextOnLocalRuntime(DeprecatedContextDriverConfiguration());
        }

        private void TestContextOnLocalRuntime(IConfiguration configuration)
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            CleanUp(testFolder);
            TestRun(configuration, typeof(TestContextHandlers), 1, "testSimpleContext", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, testFolder: testFolder);
            ValidateMessageSuccessfullyLogged(ValidationMessage, testFolder);
            CleanUp(testFolder);
        }

        private static IConfiguration ContextDriverConfiguration()
        {
            return DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<TestContextHandlers>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<AllocatedEvaluatorHandler>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<TestContextHandlers>.Class)
                .Set(DriverConfiguration.OnTaskMessage, GenericType<HelloTaskMessageHandler>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<TestContextHandlers>.Class)
                .Set(DriverConfiguration.OnTaskRunning, GenericType<TestContextHandlers>.Class)
                .Build();
        }

        private static IConfiguration DeprecatedContextDriverConfiguration()
        {
            return DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<TestContextHandlers>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<DeprecatedAllocatedEvaluatorHandler>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<TestContextHandlers>.Class)
                .Set(DriverConfiguration.OnTaskMessage, GenericType<HelloTaskMessageHandler>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<TestContextHandlers>.Class)
                .Set(DriverConfiguration.OnTaskRunning, GenericType<TestContextHandlers>.Class)
                .Build();
        }

        private sealed class AllocatedEvaluatorHandler : IObserver<IAllocatedEvaluator>
        {
            [Inject]
            private AllocatedEvaluatorHandler()
            {
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                value.SubmitContext(ContextConfiguration.ConfigurationModule
                    .Set(ContextConfiguration.Identifier, ContextId)
                    .Build());
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

        private sealed class DeprecatedAllocatedEvaluatorHandler : IObserver<IAllocatedEvaluator>
        {
            [Inject]
            private DeprecatedAllocatedEvaluatorHandler()
            {
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                value.SubmitContext(REEF.Driver.Context.ContextConfiguration.ConfigurationModule
                    .Set(REEF.Driver.Context.ContextConfiguration.Identifier, ContextId)
                    .Build());
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

        private sealed class TestContextHandlers : IObserver<IDriverStarted>, IObserver<IActiveContext>, IObserver<IRunningTask>, IObserver<ICompletedTask>
        {
            private readonly IEvaluatorRequestor _requestor;

            [Inject]
            private TestContextHandlers(IEvaluatorRequestor evaluatorRequestor)
            {
                _requestor = evaluatorRequestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().Build());
            }

            public void OnNext(IActiveContext value)
            {
                Logger.Log(Level.Info, "ContextId: " + value.Id);
                if (value.Id != ContextId)
                {
                    throw new Exception("Unexpected ContextId: " + value.Id);
                }

                value.SubmitTask(
                    TaskConfiguration.ConfigurationModule.Set(TaskConfiguration.Identifier, "helloTaskId")
                    .Set(TaskConfiguration.Task, GenericType<HelloTask>.Class)
                    .Build());
            }

            public void OnNext(IRunningTask value)
            {
                Logger.Log(Level.Info, "Running Task" + value.Id);
            }

            public void OnNext(ICompletedTask value)
            {
                Logger.Log(Level.Info, "Completed Task" + value.Id);
                Logger.Log(Level.Info, ValidationMessage);
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
        }
    }
}