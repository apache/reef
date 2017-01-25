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
using System.Threading;
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
    public sealed class UnhandledThreadExceptionInTaskTest : ReefFunctionalTest
    {
        private const string ExpectedEvaluatorFailureMessage = "Unhandled Exception.";
        private const string ExpectedTaskId = "TaskID";
        private const string SerializableSuccessMessage = "Evaluator successfully received serializable unhandled Exception.";
        private const string NonSerializableSuccessMessage = "Evaluator successfully received nonserializable unhandled Exception.";

        /// <summary>
        /// This test validates that an unhandled Thread Exception in a user's Task crashes the Evaluator and 
        /// the Evaluator does an attempt to send a final message to the Driver.
        /// </summary>
        [Fact]
        public void TestUnhandledThreadExceptionCrashesEvaluator()
        {
            var testFolder = DefaultRuntimeFolder + TestId;
            TestRun(GetDriverConfiguration(), typeof(UnhandledThreadExceptionInTaskTest), 1, "testUnhandledThreadException", "local", testFolder);
            ValidateSuccessForLocalRuntime(0, numberOfEvaluatorsToFail: 2, testFolder: testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(SerializableSuccessMessage, testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(NonSerializableSuccessMessage, testFolder);
            CleanUp(testFolder);
        }

        private static IConfiguration GetDriverConfiguration()
        {
            return DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<UnhandledThreadExceptionInTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorCompleted, GenericType<UnhandledThreadExceptionInTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<UnhandledThreadExceptionInTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<UnhandledThreadExceptionInTaskTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<UnhandledThreadExceptionInTaskTestDriver>.Class)
                .Build();
        }

        /// <summary>
        /// This Task throws an unhandled Exception in the thread that it spins off to
        /// trigger an Evaluator failure.
        /// </summary>
        private sealed class UnhandledThreadExceptionInTaskTestTask : ITask
        {
            private readonly bool _shouldThrowSerializableException;

            [Inject]
            private UnhandledThreadExceptionInTaskTestTask(
                [Parameter(typeof(ShouldThrowSerializableException))] bool shouldThrowSerializableException)
            {
                _shouldThrowSerializableException = shouldThrowSerializableException;
            }

            public byte[] Call(byte[] memento)
            {
                var thread = new Thread(() =>
                {
                    if (_shouldThrowSerializableException)
                    {
                        throw new TestSerializableException(ExpectedEvaluatorFailureMessage);
                    }
                    else
                    {
                        throw new TestNonSerializableException(ExpectedEvaluatorFailureMessage);
                    }
                });

                thread.Start();
                thread.Join();
                return null;
            }

            public void Dispose()
            {
            }
        }

        /// <summary>
        /// This Driver verifies that the unhandled Exception triggers an Evaluator failure
        /// and verifies the type of Exception and its message.
        /// </summary>
        private sealed class UnhandledThreadExceptionInTaskTestDriver : 
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>, 
            IObserver<IFailedEvaluator>,
            IObserver<ICompletedEvaluator>,
            IObserver<ICompletedTask>
        {
            private static readonly Logger Logger = Logger.GetLogger(typeof(UnhandledThreadExceptionInTaskTestDriver));

            private readonly IEvaluatorRequestor _evaluatorRequestor;
            private bool _shouldReceiveSerializableException = true;

            [Inject]
            private UnhandledThreadExceptionInTaskTestDriver(IEvaluatorRequestor evaluatorRequestor)
            {
                _evaluatorRequestor = evaluatorRequestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _evaluatorRequestor.Submit(
                    _evaluatorRequestor.NewBuilder()
                        .SetCores(1)
                        .SetNumber(1)
                        .Build());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                var taskConf = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, ExpectedTaskId)
                    .Set(TaskConfiguration.Task, GenericType<UnhandledThreadExceptionInTaskTestTask>.Class)
                    .Build();

                var shouldThrowSerializableConfig = TangFactory.GetTang().NewConfigurationBuilder()
                    .BindNamedParameter<ShouldThrowSerializableException, bool>(
                        GenericType<ShouldThrowSerializableException>.Class, _shouldReceiveSerializableException.ToString())
                    .Build();

                value.SubmitTask(Configurations.Merge(taskConf, shouldThrowSerializableConfig));
            }

            public void OnNext(ICompletedTask value)
            {
                throw new Exception("Driver should not have received a completed Task.");
            }

            public void OnNext(ICompletedEvaluator value)
            {
                throw new Exception("Driver should not have received a completed Evaluator.");
            }

            public void OnNext(IFailedEvaluator value)
            {
                if (value.EvaluatorException == null)
                {
                    throw new Exception("Evaluator should contain a valid Exception.");
                }

                if (!value.EvaluatorException.Message.Contains(ExpectedEvaluatorFailureMessage))
                {
                    throw new Exception("Evaluator expected to contain the message " + ExpectedEvaluatorFailureMessage);
                }

                if (!value.FailedTask.IsPresent())
                {
                    throw new Exception("Failed task should be present.");
                }

                if (value.FailedTask.Value.Id != ExpectedTaskId)
                {
                    throw new Exception("Failed Task does not have the right Task ID.");
                }

                if (_shouldReceiveSerializableException)
                {
                    var serializableEx = value.EvaluatorException.InnerException as TestSerializableException;
                    if (serializableEx == null)
                    {
                        throw new Exception("Evaluator InnerException expected to be of type " + typeof(TestSerializableException).Name);
                    }

                    if (!serializableEx.Message.Equals(ExpectedEvaluatorFailureMessage))
                    {
                        throw new Exception("Evaluator InnerException.Message expected to be " + ExpectedEvaluatorFailureMessage);
                    }

                    _shouldReceiveSerializableException = false;
                    Logger.Log(Level.Info, SerializableSuccessMessage);

                    _evaluatorRequestor.Submit(
                        _evaluatorRequestor.NewBuilder()
                            .SetCores(1)
                            .SetNumber(1)
                            .Build());
                }
                else
                {
                    var nonSerializableEx = value.EvaluatorException.InnerException as NonSerializableEvaluatorException;
                    if (nonSerializableEx == null)
                    {
                        throw new Exception("Evaluator Exception expected to be of type " + typeof(NonSerializableEvaluatorException));
                    }

                    if (!nonSerializableEx.Message.Contains(ExpectedEvaluatorFailureMessage))
                    {
                        throw new Exception("Evaluator InnerException.Message expected to contain the message " + ExpectedEvaluatorFailureMessage);
                    }

                    Logger.Log(Level.Info, NonSerializableSuccessMessage);
                }
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