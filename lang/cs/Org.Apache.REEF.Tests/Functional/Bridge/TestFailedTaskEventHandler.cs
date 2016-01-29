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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
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
    [Collection("FunctionalTests")]
    public sealed class TestFailedTaskEventHandler : ReefFunctionalTest
    {
        private const string FailedTaskMessage = "I have successfully seen a failed task.";

        public TestFailedTaskEventHandler()
        {
            Init();
        }

        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test invocation of FailedTaskHandler")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestFailedTaskEventHandlerOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            CleanUp(testFolder);
            TestRun(DriverConfigurations(), typeof(FailedTaskDriver), 1, "failedTaskTest", "local", testFolder);
            ValidateSuccessForLocalRuntime(numberOfEvaluatorsToClose: 1, numberOfTasksToFail: 1, testFolder: testFolder);
            ValidateMessageSuccessfullyLogged(FailedTaskMessage, testFolder);
            CleanUp(testFolder);
        }

        public IConfiguration DriverConfigurations()
        {
            var driverConfig = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<FailedTaskDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<FailedTaskDriver>.Class)
                .Set(DriverConfiguration.OnTaskFailed, GenericType<FailedTaskDriver>.Class)
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(driverConfig)
                .BindSetEntry<DriverBridgeConfigurationOptions.SetOfAssemblies, string>(typeof(FailTask).Assembly.GetName().Name)
                .Build();
        }

        private sealed class FailedTaskDriver : IObserver<IDriverStarted>, IObserver<IAllocatedEvaluator>, 
            IObserver<IFailedTask>, IObserver<ICompletedTask>
        {
            private static readonly Logger Logger = Logger.GetLogger(typeof(FailedTaskDriver));

            private readonly IEvaluatorRequestor _requestor;

            [Inject]
            private FailedTaskDriver(IEvaluatorRequestor requestor)
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
                    .Set(TaskConfiguration.Identifier, "1234567")
                    .Set(TaskConfiguration.Task, GenericType<FailTask>.Class)
                    .Build());
            }

            public void OnNext(IFailedTask value)
            {
                Logger.Log(Level.Error, FailedTaskMessage);
                value.GetActiveContext().Value.Dispose();
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
        }

        private sealed class FailTask : ITask
        {
            [Inject]
            private FailTask()
            {
            }

            public void Dispose()
            {
            }

            public byte[] Call(byte[] memento)
            {
                throw new Exception("Expected exception.");
            }
        }
    }
}
