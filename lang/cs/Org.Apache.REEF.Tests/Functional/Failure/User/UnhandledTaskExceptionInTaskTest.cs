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
using System.Threading.Tasks;
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
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Failure.User
{
    [Collection("FunctionalTests")]
    public sealed class UnhandledTaskExceptionInTaskTest : ReefFunctionalTest
    {
        private const string FailedTaskMessage = "I have successfully seen all failed tasks.";
        private const string ExpectedExceptionMessage = "Expected exception.";

        /// <summary>
        /// This test validates that an unhandled Task Exception in a user's Task doesn't crash the Evaluator.
        /// Instead, the exception is propagated as a regular Task exception.
        /// </summary>
        [Fact]
        public void TestUnhandledTaskExceptionDoesntCrashEvaluator()
        {
            var testFolder = DefaultRuntimeFolder + TestId;
            TestRun(GetDriverConfiguration(), typeof(UnhandledThreadExceptionInTaskTest), 1, "testUnhandledTaskException", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, 2, testFolder: testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(FailedTaskMessage, testFolder);
            CleanUp(testFolder);
        }

        private static IConfiguration GetDriverConfiguration()
        {
            return DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<UnhandledTaskExceptionInTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<UnhandledTaskExceptionInTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskFailed, GenericType<UnhandledTaskExceptionInTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<UnhandledTaskExceptionInTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<UnhandledTaskExceptionInTaskTestDriver>.Class)
                .Build();
        }

        /// <summary>
        /// This Task throws an unhandled Exception in a Threading.Tasks.Task that it spins off.
        /// </summary>
        private sealed class UnhandledTaskExceptionInTaskTestTask : ITask
        {
            private readonly bool _shouldThrowSerializableException;

            [Inject]
            private UnhandledTaskExceptionInTaskTestTask(
                [Parameter(typeof(ShouldThrowSerializableException))] bool shouldThrowSerializableException)
            {
                _shouldThrowSerializableException = shouldThrowSerializableException;
            }

            public byte[] Call(byte[] memento)
            {
                Task task = Task.Run(() =>
                {
                    if (_shouldThrowSerializableException)
                    {
                        throw new TestSerializableException(ExpectedExceptionMessage);
                    }
                    throw new TestNonSerializableException(ExpectedExceptionMessage);
                });

                task.Wait();
                return null;
            }

            public void Dispose()
            {
            }
        }

        /// <summary>
        /// This Driver verifies that the unhandled Exception doesn't trigger an Evaluator failure
        /// but a failed Task instead.
        /// </summary>
        private sealed class UnhandledTaskExceptionInTaskTestDriver : IObserver<IDriverStarted>, IObserver<IAllocatedEvaluator>,
            IObserver<IFailedTask>, IObserver<ICompletedTask>, IObserver<IFailedEvaluator>
        {
            private const string TaskId = "1234567";

            private static readonly Logger Logger = Logger.GetLogger(typeof(UnhandledTaskExceptionInTaskTestDriver));

            private readonly IEvaluatorRequestor _requestor;

            private bool _shouldReceiveSerializableException;
            private int _numFailedTasksReceived;

            [Inject]
            private UnhandledTaskExceptionInTaskTestDriver(IEvaluatorRequestor requestor)
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

            public void OnNext(IFailedEvaluator value)
            {
                throw new Exception("Didn't expect a failed Evaluator.");
            }

            public void OnNext(IFailedTask value)
            {
                _numFailedTasksReceived++;

                if (value.Id != TaskId)
                {
                    throw new Exception("Received Task ID " + value.Id + " instead of the expected Task ID " + TaskId);
                }

                // since in this test exception is thrown by Threading.Tasks.Task spawned in our Task
                // the exception is wrapped in AggregateException with "One or more errors occurred." message
                if (value.Message == null || value.Message != "One or more errors occurred.")
                {
                    throw new Exception("Exception message not properly propagated. Received message " + value.Message);
                }

                if (_shouldReceiveSerializableException)
                {
                    if (value.AsError() == null || !(value.AsError() is AggregateException))
                    {
                        throw new Exception("Outer exception should have been an AggregateException. " + value.AsError());
                    }
                    var inner = value.AsError().InnerException;

                    if (inner == null || !(inner is TestSerializableException))
                    {
                        throw new Exception("Exception should have been serialized properly.");
                    }

                    if (inner.Message != ExpectedExceptionMessage)
                    {
                        throw new Exception("Incorrect Exception message, got message: " + value.AsError().Message);
                    }

                    value.GetActiveContext().Value.Dispose();

                    if (_numFailedTasksReceived == 2)
                    {
                        Logger.Log(Level.Error, FailedTaskMessage);
                    }
                }
                else
                {
                    var nonSerializableTaskException = value.AsError() as NonSerializableTaskException;
                    if (nonSerializableTaskException == null)
                    {
                        throw new Exception(
                            "Expected a NonSerializableTaskException from Task, instead got Exception of type " + value.AsError().GetType());
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
                    .Set(TaskConfiguration.Task, GenericType<UnhandledTaskExceptionInTaskTestTask>.Class)
                    .Build();

                return Configurations.Merge(shouldThrowSerializableConfig, taskConfig);
            }
        }
    }
}