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
    /// This test class contains a test that validates that an Exception in 
    /// a TaskStartHandler triggers an <see cref="IFailedTask"/> event.
    /// </summary>
    [Collection("FunctionalTests")]
    public sealed class TaskStartExceptionTest : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TaskStartExceptionTest));

        private const string TaskStartExceptionMessage = "TaskStartExceptionMessage";
        private const string InitialTaskMessage = "InitialTaskMessage";
        private const string ResubmitTaskMessage = "ResubmitTaskMessage";
        private const string FailedTaskReceived = "FailedTaskReceived";
        private const string CompletedTaskReceived = "CompletedTaskReceived";

        /// <summary>
        /// This test validates that an Exception in the TaskStartHandler causes a FailedTask
        /// event in the Driver, and that a new Task can be submitted on the original Context.
        /// </summary>
        [Fact]
        public void TestStopTaskWithExceptionOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<TaskStartExceptionTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<TaskStartExceptionTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<TaskStartExceptionTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskFailed, GenericType<TaskStartExceptionTestDriver>.Class)
                .Build(), typeof(TaskStartExceptionTestDriver), 1, "testStartTaskWithExceptionOnLocalRuntime", "local", testFolder);

            var driverMessages = new List<string>
            {
                FailedTaskReceived,
                CompletedTaskReceived
            };

            ValidateMessagesSuccessfullyLoggedForDriver(driverMessages, testFolder, 1);

            // Validate that the first Task never starts.
            ValidateMessageSuccessfullyLogged(
                new List<string> { InitialTaskMessage }, "Node-*", EvaluatorStdout, testFolder, 0);

            ValidateMessageSuccessfullyLogged(
                new List<string> { ResubmitTaskMessage }, "Node-*", EvaluatorStdout, testFolder, 1);

            CleanUp(testFolder);
        }

        private sealed class TaskStartExceptionTestDriver :
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<ICompletedTask>,
            IObserver<IFailedTask>
        {
            private readonly IEvaluatorRequestor _requestor;

            [Inject]
            private TaskStartExceptionTestDriver(IEvaluatorRequestor requestor)
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
                        .Set(TaskConfiguration.Task, GenericType<TaskStartExceptionTask>.Class)
                        .Set(TaskConfiguration.OnTaskStart, GenericType<TaskStartHandlerWithException>.Class)
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

                var taskStartEx = ex as TaskStartExceptionTestException;

                Assert.True(taskStartEx != null, "Expected Exception to be of type TaskStartExceptionTestException, but instead got type " + ex.GetType().Name);
                Assert.True(taskStartEx.Message.Equals(TaskStartExceptionMessage),
                    "Expected message to be " + TaskStartExceptionMessage + " but instead got " + taskStartEx.Message + ".");

                Logger.Log(Level.Info, FailedTaskReceived);

                // Submit the new Task to verify that the original Context accepts new Tasks.
                value.GetActiveContext().Value.SubmitTask(
                    TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier, "TaskID")
                        .Set(TaskConfiguration.Task, GenericType<TaskStartExceptionResubmitTask>.Class)
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

        private sealed class TaskStartExceptionTask : LoggingTask
        {
            [Inject]
            private TaskStartExceptionTask()
                : base(InitialTaskMessage)
            {
            }
        }

        /// <summary>
        /// A simple Task for Task resubmission validation on a Context with a previous Task
        /// that failed on a Task StartHandler.
        /// </summary>
        private sealed class TaskStartExceptionResubmitTask : LoggingTask
        {
            [Inject]
            private TaskStartExceptionResubmitTask()
                : base(ResubmitTaskMessage)
            {
            }
        }

        /// <summary>
        /// Throws a test Exception on Task Start to trigger a task failure.
        /// </summary>
        private sealed class TaskStartHandlerWithException : ExceptionThrowingHandler<ITaskStart>
        {
            [Inject]
            private TaskStartHandlerWithException() :
                base(new TaskStartExceptionTestException(TaskStartExceptionMessage))
            {
            }
        }

        /// <summary>
        /// A Serializable Exception to verify that the Exception is deserialized correctly.
        /// </summary>
        [Serializable]
        private sealed class TaskStartExceptionTestException : Exception
        {
            public TaskStartExceptionTestException(string message)
                : base(message)
            {
            }

            private TaskStartExceptionTestException(SerializationInfo info, StreamingContext context)
                : base(info, context)
            {
            }
        }
    }
}