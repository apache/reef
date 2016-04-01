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
using System.Collections.Generic;
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
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.FaultTolerant
{
    /// <summary>
    /// This test case servers as an example to put data downloading at part of the ContextStartHandler
    /// </summary>
    public class TestContextStart : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TestContextStart));
        private const string StartedHandlerMessage = "Start Handler is called.";
        private const string StartedMessage = "Do something started.";
        private const string CompletedMessage = "Do something completed.";

        /// <summary>
        /// This test case submit a context with a Context start handler and do something in the handler
        /// </summary>
        [Fact]
        public void TestDosomethingOnContextStartOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurations(), typeof(ContextStartDriver), 1, "ContextStartDriver", "local", testFolder);
            ValidateSuccessForLocalRuntime(2, testFolder: testFolder);

            var messages = new List<string>();
            messages.Add(StartedMessage);
            messages.Add(StartedHandlerMessage);
            ValidateMessageSuccessfullyLogged(messages, "Node-*", EvaluatorStdout, testFolder, 2);
            CleanUp(testFolder);
        }

        public IConfiguration DriverConfigurations()
        {
            return DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<ContextStartDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<ContextStartDriver>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<ContextStartDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<ContextStartDriver>.Class)
                .Set(DriverConfiguration.OnContextClosed, GenericType<ContextStartDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorCompleted, GenericType<ContextStartDriver>.Class)
                .Build();
        }

        private sealed class ContextStartDriver :
             IObserver<IDriverStarted>,
             IObserver<IAllocatedEvaluator>,
             IObserver<IActiveContext>,
             IObserver<ICompletedTask>,
             IObserver<IClosedContext>,
             IObserver<ICompletedEvaluator>
        {
            private readonly IEvaluatorRequestor _requestor;
            private const string ContextId1 = "ContextID1";
            private const string ContextId2 = "ContextID2";
            private const string TaskId = "TaskID";
            private bool _first = true;

            [Inject]
            private ContextStartDriver(IEvaluatorRequestor evaluatorRequestor)
            {
                _requestor = evaluatorRequestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().Build());
            }

            public void OnNext(IActiveContext value)
            {               
                Logger.Log(Level.Info, "IActiveContext: " + value.Id);

                if (_first)
                {
                    Assert.Equal(value.Id, ContextId1);
                    _first = false;
                    value.SubmitContext(
                        ContextConfiguration.ConfigurationModule
                            .Set(ContextConfiguration.Identifier, ContextId2)
                            .Set(ContextConfiguration.OnContextStart, GenericType<ContextStartHandler>.Class)
                            .Build());
                }
                else
                {
                    Assert.Equal(value.Id, ContextId2);
                    var c = TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, TaskId)
                        .Set(TaskConfiguration.Task, GenericType<TestTask>.Class)
                        .Build();
                    value.SubmitTask(c);
                }
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                value.SubmitContext(
                    ContextConfiguration.ConfigurationModule
                        .Set(ContextConfiguration.Identifier, ContextId1)
                        .Set(ContextConfiguration.OnContextStart, GenericType<ContextStartHandler>.Class)
                        .Build());
            }

            public void OnNext(ICompletedTask value)
            {
                Logger.Log(Level.Info, "Task is completed:" + value.Id);
                Assert.Equal(value.Id, TaskId);
                value.ActiveContext.Dispose();
            }

            public void OnNext(IClosedContext value)
            {
                Logger.Log(Level.Info, "Second context is closed: " + value.Id);
                Assert.Equal(value.Id, ContextId2);
                Assert.Equal(value.ParentContext.Id, ContextId1);
                value.ParentContext.Dispose();
            }

            public void OnNext(ICompletedEvaluator value)
            {
                Logger.Log(Level.Info, "In CompletedEvaluator " + value.Id);
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

        private sealed class ContextStartHandler : IObserver<IContextStart>
        {
            private readonly DoSomething _doSomething;

            [Inject]
            private ContextStartHandler(DoSomething dataDownLoader)
            {
                _doSomething = dataDownLoader;
            }

            public void OnNext(IContextStart value)
            {
                Logger.Log(Level.Info, StartedHandlerMessage);
                _doSomething.DoIt();
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

        private sealed class DoSomething
        {
            private bool _done;

            [Inject]
            private DoSomething()
            {
                _done = false;
            }

            public void DoIt()
            {
                Logger.Log(Level.Info, StartedMessage);
                _done = true;
            }

            public bool Done
            {
                get { return _done; }
            }
        }

        private sealed class TestTask : ITask
        {
            private readonly DoSomething _dataDownLoader;

            [Inject]
            private TestTask(DoSomething dataDownLoader)
            {
                _dataDownLoader = dataDownLoader;
            }

            public void Dispose()
            {
            }

            public byte[] Call(byte[] memento)
            {
                Logger.Log(Level.Info, "Hello in TestTask");
                if (_dataDownLoader.Done == true)
                {
                    Logger.Log(Level.Info, CompletedMessage);
                    return null;
                }
                return null;
            }
        }
    }
}