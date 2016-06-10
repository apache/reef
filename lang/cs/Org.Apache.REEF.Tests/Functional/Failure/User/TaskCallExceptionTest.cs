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
using System.Runtime.Serialization;
using Org.Apache.REEF.Common.Exceptions;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Tests.Functional.Bridge.Exceptions;
using Org.Apache.REEF.Tests.Functional.Bridge.Parameters;
using Org.Apache.REEF.Tests.Functional.Common.Task;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Failure.User
{
    [Collection("FunctionalTests")]
    public sealed class TaskCallExceptionTest : ReefFunctionalTest
    {
        private const string FailedTaskMessage = "I have successfully seen all failed tasks.";
        private const string ExpectedExceptionMessage = "Expected exception.";
        private const int NumFailedTasksExpected = 2;

        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test invocation of FailedTaskHandler. Validates the Task ID of the failure, as well as the Exceptions of the Task failure.")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestFailedTaskEventHandlerOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(
                DriverConfiguration.ConfigurationModule
                    .Set(DriverConfiguration.OnDriverStarted, GenericType<TaskCallExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<TaskCallExceptionDriver>.Class)
                    .Set(DriverConfiguration.OnTaskFailed, GenericType<TaskCallExceptionDriver>.Class)
                    .Build(), 
                typeof(TaskCallExceptionDriver), 1, "failedTaskTest", "local", testFolder);

            ValidateSuccessForLocalRuntime(numberOfContextsToClose: 1, numberOfTasksToFail: NumFailedTasksExpected, testFolder: testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(FailedTaskMessage, testFolder);
            CleanUp(testFolder);
        }

        private sealed class TaskCallExceptionDriver : IObserver<IDriverStarted>, IObserver<IAllocatedEvaluator>, 
            IObserver<IFailedTask>, IObserver<ICompletedTask>
        {
            private const string TaskId = "1234567";

            private static readonly Logger Logger = Logger.GetLogger(typeof(TaskCallExceptionDriver));

            private readonly IEvaluatorRequestor _requestor;

            private bool _shouldReceiveSerializableException = false;
            private int _numFailedTasksReceived = 0;

            [Inject]
            private TaskCallExceptionDriver(IEvaluatorRequestor requestor)
            {
                _requestor = requestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().Build());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                value.SubmitTask(GetTaskConfiguration());
            }

            public void OnNext(IFailedTask value)
            {
                _numFailedTasksReceived++;

                if (value.Id != TaskId)
                {
                    throw new Exception("Received Task ID " + value.Id + " instead of the expected Task ID " + TaskId);
                }

                if (value.Message == null || value.Message != ExpectedExceptionMessage)
                {
                    throw new Exception("Exception message not properly propagated. Received message " + value.Message);
                }

                if (_shouldReceiveSerializableException)
                {
                    if (_numFailedTasksReceived == NumFailedTasksExpected)
                    {
                        Logger.Log(Level.Error, FailedTaskMessage);
                    }

                    if (value.AsError() == null || !(value.AsError() is TestSerializableException))
                    {
                        throw new Exception("Exception should have been serialized properly.");
                    }

                    if (value.AsError().Message != ExpectedExceptionMessage)
                    {
                        throw new Exception("Incorrect Exception message, got message: " + value.AsError().Message);
                    }

                    value.GetActiveContext().Value.Dispose();
                }
                else
                {
                    var taskException = value.AsError();
                    if (taskException == null)
                    {
                        throw new Exception("Expected a non-null task exception.");
                    }

                    var nonSerializableTaskException = taskException as NonSerializableTaskException;
                    if (nonSerializableTaskException == null)
                    {
                        throw new Exception(
                            "Expected a NonSerializableTaskException from Task, instead got Exception of type " + taskException.GetType());
                    }

                    if (!(nonSerializableTaskException.InnerException is SerializationException))
                    {
                        throw new Exception("Expected a SerializationException as the inner Exception of the Task Exception.");
                    }

                    _shouldReceiveSerializableException = true;
                    value.GetActiveContext().Value.SubmitTask(GetTaskConfiguration());
                }
            }

            public void OnNext(ICompletedTask value)
            {
                throw new Exception("Did not expect a completed task.");
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }

            private IConfiguration GetTaskConfiguration()
            {
                var shouldThrowSerializableConfig = TangFactory.GetTang().NewConfigurationBuilder()
                    .BindNamedParameter<ShouldThrowSerializableException, bool>(
                        GenericType<ShouldThrowSerializableException>.Class, _shouldReceiveSerializableException.ToString())
                    .Build();

                var taskConfig = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, TaskId)
                    .Set(TaskConfiguration.Task, GenericType<TaskCallExceptionTask>.Class)
                    .Build();

                return Configurations.Merge(shouldThrowSerializableConfig, taskConfig);
            }
        }

        private sealed class TaskCallExceptionTask : ExceptionTask
        {
            [Inject]
            private TaskCallExceptionTask(
                [Parameter(typeof(ShouldThrowSerializableException))] bool shouldThrowSerializableException) 
                : base(ExceptionToThrow(shouldThrowSerializableException))
            {
            }

            private static Exception ExceptionToThrow(bool shouldThrowSerializableException)
            {
                if (shouldThrowSerializableException)
                {
                    return new TestSerializableException(ExpectedExceptionMessage);
                }
                
                return new TestNonSerializableException(ExpectedExceptionMessage);
            }
        }
    }
}
