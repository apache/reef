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
using Org.Apache.REEF.Common.Runtime.Evaluator.Task;
using Org.Apache.REEF.Common.Tasks;
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
    /// This test class contains a test that validates that an Exception in an <see cref="ITask"/> 
    /// constructor triggers an <see cref="IFailedTask"/> event.
    /// </summary>
    [Collection("FunctionalTests")]
    public sealed class TaskConstructorExceptionTest : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TaskConstructorExceptionTest));

        private const string TaskId = "TaskID";
        private const string ContextId = "ContextID";

        /// <summary>
        /// This test validates that an Exception in an <see cref="ITask"/> constructor triggers
        /// a <see cref="IFailedTask"/> event.
        /// </summary>
        [Fact]
        public void TestTaskConstructorException()
        {
            var testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);

            TestRun(
                DriverConfiguration.ConfigurationModule
                    .Set(DriverConfiguration.OnDriverStarted, GenericType<TestTaskConstructorExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<TestTaskConstructorExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnTaskFailed, GenericType<TestTaskConstructorExceptionDriver>.Class)
                    .Build(),
                typeof(TestTaskConstructorExceptionDriver), 1, "TestTaskConstructorException", "local", testFolder);

            ValidateSuccessForLocalRuntime(numberOfContextsToClose: 1, numberOfTasksToFail: 1, testFolder: testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(TestTaskConstructorExceptionDriver.ReceivedFailedTaskEvent, testFolder);
            CleanUp(testFolder);
        }

        private sealed class TestTaskConstructorExceptionDriver :
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<IFailedTask>
        {
            public const string ReceivedFailedTaskEvent = "ReceivedFailedTaskEvent";

            private readonly IEvaluatorRequestor _requestor;

            [Inject]
            private TestTaskConstructorExceptionDriver(IEvaluatorRequestor requestor)
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
                    .Set(TaskConfiguration.Task, GenericType<TestConstructorExceptionTask>.Class)
                    .Build();

                value.SubmitContextAndTask(contextConf, taskConf);
            }

            public void OnNext(IFailedTask value)
            {
                var taskClientCodeEx = value.AsError() as TaskClientCodeException;
                if (taskClientCodeEx == null)
                {
                    throw new Exception("Expected Exception to be a TaskClientCodeException.");
                }

                if (taskClientCodeEx.ContextId != ContextId)
                {
                    throw new Exception("Expected Context ID to be " + ContextId + ", but instead got " + taskClientCodeEx.ContextId);
                }

                if (taskClientCodeEx.TaskId != TaskId)
                {
                    throw new Exception("Expected Task ID to be " + TaskId + ", but instead got " + taskClientCodeEx.TaskId);
                }

                Exception error = taskClientCodeEx;

                var foundErrorMessage = false;
                while (error != null)
                {
                    // Using Contains because the Exception may not be serializable
                    // and the message of the wrapping Exception may be expanded to include more details.
                    if (error.Message.Contains(TestConstructorExceptionTask.ConstructorErrorMessage))
                    {
                        foundErrorMessage = true;
                        break;
                    }

                    error = error.InnerException;
                }

                if (!foundErrorMessage)
                {
                    throw new Exception("Expected to find error message " + 
                        TestConstructorExceptionTask.ConstructorErrorMessage + " in the layer of Exceptions.");
                }

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

        private sealed class TestConstructorExceptionTask : ITask
        {
            public const string ConstructorErrorMessage = "ConstructorErrorMessage";

            [Inject]
            private TestConstructorExceptionTask()
            {
                throw new Exception(ConstructorErrorMessage);
            }

            public void Dispose()
            {
            }

            public byte[] Call(byte[] memento)
            {
                throw new NotImplementedException();
            }
        }
    }
}