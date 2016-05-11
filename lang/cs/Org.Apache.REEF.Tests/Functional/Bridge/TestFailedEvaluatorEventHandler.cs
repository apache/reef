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
using System.Globalization;
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Bridge
{
    [Collection("FunctionalTests")]
    public sealed class TestFailedEvaluatorEventHandler : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TestFailedEvaluatorEventHandler));
        private const string FailedEvaluatorMessage = "I have succeeded in seeing a failed evaluator.";
        private const string RightFailedTaskMessage = "I have succeeded in seeing the right failed task.";
        private const string FailSignal = "Fail";
        private const string TaskId = "1234567";

        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test invocation of FailedEvaluatorHandler")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestFailedEvaluatorEventHandlerOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurations(), typeof(FailedEvaluatorDriver), 1, "failedEvaluatorTest", "local", testFolder);
            ValidateSuccessForLocalRuntime(0, numberOfEvaluatorsToFail: 1, testFolder: testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(FailedEvaluatorMessage, testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(RightFailedTaskMessage, testFolder);
            CleanUp(testFolder);
        }

        private IConfiguration DriverConfigurations()
        {
            return DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<FailedEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<FailedEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorCompleted, GenericType<FailedEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<FailedEvaluatorDriver>.Class)
                .Build();
        }

        private sealed class FailedEvaluatorDriver : IObserver<IDriverStarted>, IObserver<IAllocatedEvaluator>, 
            IObserver<ICompletedEvaluator>, IObserver<IFailedEvaluator>
        {
            private static readonly Logger Logger = Logger.GetLogger(typeof(FailedEvaluatorDriver));

            private readonly IEvaluatorRequestor _requestor;

            [Inject]
            private FailedEvaluatorDriver(IEvaluatorRequestor requestor)
            {
                _requestor = requestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().Build());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                value.SubmitTask(TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, TaskId)
                    .Set(TaskConfiguration.Task, GenericType<FailEvaluatorTask>.Class)
                    .Build());
            }

            public void OnNext(ICompletedEvaluator value)
            {
                throw new Exception("Did not expect completed evaluator.");
            }

            public void OnNext(IFailedEvaluator value)
            {
                Logger.Log(Level.Error, FailedEvaluatorMessage);
                Assert.True(value.FailedTask.IsPresent(), "No failed task found");
                Assert.Equal(TaskId, value.FailedTask.Value.Id);
                Assert.Equal(1, value.FailedContexts.Count);
                Assert.Equal(value.Id, value.EvaluatorException.EvaluatorId);
                Logger.Log(Level.Error, string.Format(CultureInfo.CurrentCulture, "Failed task id:{0}, failed Evaluator id: {1}, Failed Exception msg: {2},", value.FailedTask.Value.Id, value.EvaluatorException.EvaluatorId, value.EvaluatorException.Message));
                Logger.Log(Level.Error, RightFailedTaskMessage);
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

        private sealed class FailEvaluatorTask : ITask
        {
            [Inject]
            private FailEvaluatorTask()
            {
            }

            public void Dispose()
            {
            }

            public byte[] Call(byte[] memento)
            {
                Logger.Log(Level.Info, "Entered FailEvaluatorTask");

                // need to sleep to allow Java code to get heartbeat with information about task
                Thread.Sleep(100);
                Environment.Exit(1);
                return null;
            }
        }
    }
}