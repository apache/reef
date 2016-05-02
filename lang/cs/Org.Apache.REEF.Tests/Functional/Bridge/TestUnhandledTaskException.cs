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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Bridge
{
    [Collection("FunctionalTests")]
    public sealed class TestUnhandledTaskException : ReefFunctionalTest
    {
        private const string ExpectedEvaluatorFailureMessage = "Unhandled Exception.";
        private const string ExpectedTaskId = "TaskID";
        private const string SuccessMessage = "Evaluator successfully received unhandled Exception.";

        /// <summary>
        /// This test validates that an unhandled Task Exception crashes the Evaluator and the Evaluator
        /// does an attempt to send a final message to the Driver.
        /// TODO[JIRA REEF-1286]: Currently, this only validates the first portion, but does not yet validate the final message.
        /// TODO[JIRA REEF-1286]: The verification of the final message can be done when the Exceptions are serializable.
        /// </summary>
        [Fact]
        public void TestUnhandledTaskExceptionCrashesEvaluator()
        {
            var testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(GetDriverConfiguration(), typeof(TestUnhandledTaskException), 1, "testUnhandledTaskException", "local", testFolder);
            ValidateSuccessForLocalRuntime(0, numberOfEvaluatorsToFail: 1, testFolder: testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(SuccessMessage, testFolder, 1);
        }

        private static IConfiguration GetDriverConfiguration()
        {
            return DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<UnhandledExceptionTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorCompleted, GenericType<UnhandledExceptionTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<UnhandledExceptionTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<UnhandledExceptionTestDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<UnhandledExceptionTestDriver>.Class)
                .Build();
        }

        /// <summary>
        /// This Task throws an unhandled Exception in the thread that it spins off to
        /// trigger an Evaluator failure.
        /// </summary>
        private sealed class UnhandledExceptionTestTask : ITask
        {
            [Inject]
            private UnhandledExceptionTestTask()
            {
            }

            public byte[] Call(byte[] memento)
            {
                var thread = new Thread(() =>
                {
                    throw new Exception(ExpectedEvaluatorFailureMessage);
                });

                thread.Start();
                thread.Join();
                return null;
            }

            public void Dispose()
            {
                throw new NotImplementedException();
            }
        }

        /// <summary>
        /// This Driver verifies that the unhandled Exception triggers an Evaluator failure
        /// and verifies the type of Exception and its message.
        /// </summary>
        private sealed class UnhandledExceptionTestDriver : 
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>, 
            IObserver<IFailedEvaluator>,
            IObserver<ICompletedEvaluator>,
            IObserver<ICompletedTask>
        {
            private static readonly Logger Logger = Logger.GetLogger(typeof(UnhandledExceptionTestDriver));

            private readonly IEvaluatorRequestor _evaluatorRequestor;

            [Inject]
            private UnhandledExceptionTestDriver(IEvaluatorRequestor evaluatorRequestor)
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
                    .Set(TaskConfiguration.Task, GenericType<UnhandledExceptionTestTask>.Class)
                    .Build();

                value.SubmitTask(taskConf);
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
                    // TODO[JIRA REEF-1286]: Verify the Exception message and the type of Exception.
                }

                if (!value.FailedTask.IsPresent())
                {
                    throw new Exception("Failed task should be present.");
                }

                if (value.FailedTask.Value.Id != ExpectedTaskId)
                {
                    throw new Exception("Failed Task does not have the right Task ID.");
                }

                Logger.Log(Level.Info, SuccessMessage);
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