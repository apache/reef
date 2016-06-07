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
using System.Runtime.Serialization;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Runtime.Evaluator.Task;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Failure.User
{
    /// <summary>
    /// This test class contains a test that validates that an Exception in 
    /// a TaskStartHandler triggers an <see cref="IFailedTask"/> event.
    /// </summary>
    [Collection("FunctionalTests")]
    public sealed class TaskConfigurationTaskStartExceptionTest : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TaskConfigurationTaskStartExceptionTest));

        private const string TaskStartErrorString = "TaskStartErrorString";
        private const string ReceivedFailedTaskEvent = "ReceivedFailedTaskEvent";
        private const string TaskNotExpectedToStartString = "Task was not expected to start!";

        private const string TaskId = "TaskID";
        private const string ContextId = "ContextID";
        
        [Fact]
        public void TestTaskConfigurationTaskStartException()
        {
            var testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);

            TestRun(
                DriverConfiguration.ConfigurationModule
                    .Set(DriverConfiguration.OnDriverStarted, GenericType<TestTaskConfigurationTaskStartExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<TestTaskConfigurationTaskStartExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnTaskFailed, GenericType<TestTaskConfigurationTaskStartExceptionDriver>.Class)
                    .Build(),
                typeof(TestTaskConfigurationTaskStartExceptionDriver), 1, "TestTaskStartException", "local", testFolder);

            ValidateSuccessForLocalRuntime(numberOfContextsToClose: 1, numberOfTasksToFail: 1, testFolder: testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(ReceivedFailedTaskEvent, testFolder);
            
            // Not expecting TaskNotExpectedToStartString
            ValidateMessageSuccessfullyLogged(
                new List<string> { TaskNotExpectedToStartString }, "Node-*", EvaluatorStdout, testFolder, 0);

            CleanUp(testFolder);
        }

        private sealed class TestTaskConfigurationTaskStartExceptionDriver :
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<IFailedTask>
        {
            private readonly IEvaluatorRequestor _requestor;

            [Inject]
            private TestTaskConfigurationTaskStartExceptionDriver(IEvaluatorRequestor requestor)
            {
                _requestor = requestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().Build());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                var contextConf = ContextConfiguration.ConfigurationModule
                    .Set(ContextConfiguration.Identifier, ContextId)
                    .Build();

                var taskConf = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, TaskId)
                    .Set(TaskConfiguration.OnTaskStart, GenericType<TaskConfigurationTaskStartExceptionTask>.Class)
                    .Set(TaskConfiguration.Task, GenericType<TaskConfigurationTaskStartExceptionTask>.Class)
                    .Build();

                value.SubmitContextAndTask(contextConf, taskConf);
            }

            public void OnNext(IFailedTask value)
            {
                Validate.ValidateTaskClientCodeException(value, ContextId, TaskId, TaskStartErrorString);

                Logger.Log(Level.Info, ReceivedFailedTaskEvent);
                value.GetActiveContext().Value.Dispose();
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

        private sealed class TaskConfigurationTaskStartExceptionTask :
            ITask, 
            IObserver<ITaskStart>
        {
            [Inject]
            private TaskConfigurationTaskStartExceptionTask()
            {
            }

            public byte[] Call(byte[] memento)
            {
                Logger.Log(Level.Error, TaskNotExpectedToStartString);
                return null;
            }

            public void OnNext(ITaskStart value)
            {
                throw new Exception(TaskStartErrorString);
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }

            public void Dispose()
            {
            }
        }
    }
}