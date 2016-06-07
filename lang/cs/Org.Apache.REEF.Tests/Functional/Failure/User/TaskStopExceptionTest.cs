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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Tests.Functional.Common.Task;
using Org.Apache.REEF.Tests.Functional.Common.Task.Handlers;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Failure.User
{
    /// <summary>
    /// This test class contains a test that validates that an Exception in the 
    /// TaskStopHandler causes a FailedTask event in the Driver.
    /// </summary>
    [Collection("FunctionalTests")]
    public sealed class TaskStopExceptionTest : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TaskStopExceptionTest));

        private const string TaskStopExceptionMessage = "TaskStopExceptionMessage";
        private const string InitialTaskMessage = "InitialTaskMessage";
        private const string ResubmitTaskMessage = "ResubmitTaskMessage";
        private const string FailedTaskReceived = "FailedTaskReceived";
        private const string CompletedTaskReceived = "CompletedTaskReceived";

        /// <summary>
        /// This test validates that an Exception in the TaskStopHandler causes a FailedTask
        /// event in the Driver, and that a new Task can be submitted on the original Context.
        /// </summary>
        [Fact]
        public void TestStopTaskWithExceptionOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<TaskStopExceptionTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<TaskStopExceptionTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<TaskStopExceptionTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskFailed, GenericType<TaskStopExceptionTestDriver>.Class)
                .Build(), typeof(TaskStopExceptionTestDriver), 1, "testStopTaskWithExceptionOnLocalRuntime", "local", testFolder);

            var driverMessages = new List<string>
            {
                FailedTaskReceived,
                CompletedTaskReceived
            };

            ValidateMessagesSuccessfullyLoggedForDriver(driverMessages, testFolder, 1);
            ValidateMessageSuccessfullyLogged(driverMessages, "driver", DriverStdout, testFolder, 1);

            var evaluatorMessages = new List<string> { InitialTaskMessage, ResubmitTaskMessage };
            ValidateMessageSuccessfullyLogged(evaluatorMessages, "Node-*", EvaluatorStdout, testFolder, 1);
            CleanUp(testFolder);
        }

        private sealed class TaskStopExceptionTestDriver : 
            IObserver<IDriverStarted>, 
            IObserver<IAllocatedEvaluator>, 
            IObserver<ICompletedTask>, 
            IObserver<IFailedTask>
        {
            private readonly IEvaluatorRequestor _requestor;

            [Inject]
            private TaskStopExceptionTestDriver(IEvaluatorRequestor requestor)
            {
                _requestor = requestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().Build());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                // submit the first Task.
                value.SubmitTask(TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, "TaskID")
                        .Set(TaskConfiguration.Task, GenericType<TaskStopExceptionTask>.Class)
                        .Set(TaskConfiguration.OnTaskStop, GenericType<TaskStopExceptionHandler>.Class)
                        .Build());
            }

            public void OnNext(ICompletedTask value)
            {
                // Should only receive one CompletedTask, as validated.
                Logger.Log(Level.Info, CompletedTaskReceived);
                value.ActiveContext.Dispose();
            }

            public void OnNext(IFailedTask value)
            {
                // Check that Exceptions are deserialized correctly.
                var ex = value.AsError();
                if (ex == null)
                {
                    throw new Exception("Exception was not expected to be null.");
                }

                var taskStopEx = ex as TaskStopExceptionTestException;

                if (taskStopEx == null)
                {
                    throw new Exception("Expected Exception to be of type TaskStopExceptionTestException, but instead got type " + ex.GetType().Name);
                }

                if (taskStopEx.Message != TaskStopExceptionMessage)
                {
                    throw new Exception(
                        "Expected message to be " + TaskStopExceptionMessage + " but instead got " + taskStopEx.Message + ".");
                }

                Logger.Log(Level.Info, FailedTaskReceived);

                // Submit the new Task to verify that the original Context accepts new Tasks.
                value.GetActiveContext().Value.SubmitTask(
                    TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, "TaskID")
                        .Set(TaskConfiguration.Task, GenericType<TaskStopExceptionResubmitTask>.Class)
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

        private sealed class TaskStopExceptionTask : LoggingTask
        {
            [Inject]
            private TaskStopExceptionTask() 
                : base(InitialTaskMessage)
            {
            }
        }

        private sealed class TaskStopExceptionResubmitTask : LoggingTask
        {
            [Inject]
            private TaskStopExceptionResubmitTask() 
                : base(ResubmitTaskMessage)
            {
            }
        }

        private sealed class TaskStopExceptionHandler : ExceptionThrowingHandler<ITaskStop>
        {
            [Inject]
            private TaskStopExceptionHandler() : 
                base(new TaskStopExceptionTestException(TaskStopExceptionMessage))
            {
            }
        }

        /// <summary>
        /// A Serializable Exception to verify that the Exception is deserialized correctly.
        /// </summary>
        [Serializable]
        private sealed class TaskStopExceptionTestException : Exception
        {
            public TaskStopExceptionTestException(string message) : base(message)
            {
            }

            private TaskStopExceptionTestException(SerializationInfo info, StreamingContext context)
                : base(info, context)
            {
            }
        }
    }
}