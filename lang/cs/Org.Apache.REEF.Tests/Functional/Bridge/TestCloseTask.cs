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
using System.Text;
using System.Threading;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Bridge
{
    /// <summary>
    /// This test is to close a running task from driver
    /// </summary>
    public sealed class TestCloseTask : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TestCloseTask));

        private const string DisposeMessageFromDriver = "DisposeMessageFromDriver";
        private const string CompletedValidationMessage = "CompletedValidationmessage";

        public TestCloseTask()
        {
            Init();
        }

        /// <summary>
        /// This test is to close a running task over the bridge
        /// </summary>
        [Fact]
        public void TestStopTaskOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfigurations(), typeof(StopTaskHandlers), 1, "testStopTask", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, testFolder: testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(CompletedValidationMessage, testFolder, 1);
            var messages = new List<string>();
            messages.Add(DisposeMessageFromDriver);
            ValidateMessageSuccessfullyLogged(messages, "Node-*", EvaluatorStdout, testFolder, 1);
            CleanUp(testFolder);
        }

        /// <summary>
        /// Driver configuration for the test driver
        /// </summary>
        /// <returns></returns>
        public IConfiguration DriverConfigurations()
        {
            var helloDriverConfiguration = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<StopTaskHandlers>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<StopTaskHandlers>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<StopTaskHandlers>.Class)
                .Set(DriverConfiguration.OnTaskRunning, GenericType<StopTaskHandlers>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<StopTaskHandlers>.Class)
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(helloDriverConfiguration).Build();
        }

        private sealed class StopTaskHandlers :
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<IActiveContext>,
            IObserver<ICompletedTask>,
            IObserver<IRunningTask>
        {
            private readonly IEvaluatorRequestor _requestor;
            private int _contextNumber = 0;
            private int _taskNumber = 0;

            [Inject]
            private StopTaskHandlers(IEvaluatorRequestor evaluatorRequestor)
            {
                _requestor = evaluatorRequestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().SetNumber(1).Build());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                value.SubmitContext(
                    ContextConfiguration.ConfigurationModule
                        .Set(ContextConfiguration.Identifier, "ContextID" + _contextNumber++)
                        .Build());
            }

            public void OnNext(IActiveContext value)
            {
                value.SubmitTask(GetTaskConfiguration());
            }

            public void OnNext(ICompletedTask value)
            {
                // Log on task completion to signal a passed test.
                Logger.Log(Level.Info, CompletedValidationMessage + "Task completed: " + value.Id);
                value.ActiveContext.Dispose();
            }

            public void OnNext(IRunningTask value)
            {
                Logger.Log(Level.Info, "Task running: " + value.Id);
                value.Dispose(Encoding.UTF8.GetBytes(DisposeMessageFromDriver));
            }

            private IConfiguration GetTaskConfiguration()
            {
                return TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, "TaskID" + _taskNumber++)
                    .Set(TaskConfiguration.Task, GenericType<TestCloseTask.StopTestTask>.Class)
                    .Build();
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }
        }

        private sealed class StopTestTask : ITask
        {
            [Inject]
            private StopTestTask()
            {
            }

            public byte[] Call(byte[] memento)
            {
                Thread.Sleep(5 * 1000);
                return null;
            }

            public void Dispose()
            {
                Logger.Log(Level.Info, "Task is disposed.");
            }
        }
    }
}