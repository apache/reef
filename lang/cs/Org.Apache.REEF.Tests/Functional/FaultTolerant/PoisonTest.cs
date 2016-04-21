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
using System.Linq;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;
using System.Threading;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Poison;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Implementations.Configuration;

namespace Org.Apache.REEF.Tests.Functional.FaultTolerant
{
    [Collection("FunctionalTests")]
    public sealed class PoisonTest : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(PoisonTest));

        private const string Prefix = "Poison: ";
        private const string FailedEvaluatorMessage = "I have succeeded in seeing a failed evaluator.";
        private const string TaskId = "1234567";
        private const string ContextId = "ContextID";

        [Fact]
        [Trait("Description", "Test Poison functionality by injecting fault in context start handler.")]
        public void TestPoisonedEvaluatorStartHandler()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurations(), typeof(PoisonedEvaluatorDriver), 1, "poisonedEvaluatorStartTest", "local", testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(FailedEvaluatorMessage, testFolder);
            CleanUp(testFolder);
        }

        public IConfiguration DriverConfigurations()
        {
            return DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<PoisonedEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<PoisonedEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<PoisonedEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<PoisonedEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<PoisonedEvaluatorDriver>.Class)
                .Build();
        }

        private sealed class PoisonedEvaluatorDriver :
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<IActiveContext>,
            IObserver<IFailedEvaluator>,
            IObserver<ICompletedTask>
        {
            private readonly IEvaluatorRequestor _requestor;

            [Inject]
            private PoisonedEvaluatorDriver(IEvaluatorRequestor requestor)
            {
                _requestor = requestor;
            }

            public void OnNext(IDriverStarted value)
            {
                _requestor.Submit(_requestor.NewBuilder().Build());
            }

            public void OnNext(IAllocatedEvaluator value)
            {
                value.SubmitContext(ContextConfiguration.ConfigurationModule
                    .Set(ContextConfiguration.Identifier, ContextId)
                    .Build());
            }

            public void OnNext(IActiveContext value)
            {
                var taskConfig = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, TaskId)
                    .Set(TaskConfiguration.Task, GenericType<SleepTask>.Class)
                    .Set(TaskConfiguration.OnTaskStart, GenericType<PoisonedEventHandler<ITaskStart>>.Class)
                    .Build();

                var poisonConfig = TangFactory.GetTang().NewConfigurationBuilder()
                    .BindIntNamedParam<CrashTimeout>("500")
                    .BindIntNamedParam<CrashMinDelay>("100")
                    .BindNamedParameter<CrashProbability, double>(GenericType<CrashProbability>.Class, "1.0")
                    .Build();
                
                value.SubmitTask(Configurations.Merge(taskConfig, poisonConfig));
            }

            public void OnNext(IFailedEvaluator value)
            {
                Logger.Log(Level.Error, FailedEvaluatorMessage);
                if (value.FailedTask.Value == null || !value.FailedTask.IsPresent())
                {
                    throw new Exception("No failed Task associated with failed Evaluator");
                }

                if (value.FailedTask.Value.Id != TaskId)
                {
                    throw new Exception("Failed Task ID returned " + value.FailedTask.Value.Id
                        + ", was expecting Task ID " + TaskId);
                }

                Logger.Log(Level.Info, "Received all expected failed Tasks.");

                const string expectedStr = "expected a single Context with Context ID " + ContextId + ".";

                if (value.FailedContexts == null)
                {
                    throw new Exception("No Context was present but " + expectedStr);
                }

                if (value.FailedContexts.Count != 1)
                {
                    throw new Exception("Collection of failed Contexts contains " + value.FailedContexts.Count + " failed Contexts but only " + expectedStr);
                }
                
                if (!value.FailedContexts.Select(ctx => ctx.Id).Contains(ContextId))
                {
                    throw new Exception("Collection of failed Contexts does not contain expected Context ID " + ContextId + ".");
                }

                Logger.Log(Level.Info, "Received all expected failed Contexts.");
            }

            public void OnNext(ICompletedTask value)
            {
                throw new Exception("A completed task was not expected.");
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

        private sealed class SleepTask : ITask
        {
            [Inject]
            private SleepTask()
            {
            }

            public void Dispose()
            {
            }

            public byte[] Call(byte[] memento)
            {
                Logger.Log(Level.Info, Prefix + "Will sleep for 2 seconds (expecting to be poisoned faster).");
                Thread.Sleep(2000);
                Logger.Log(Level.Info, Prefix + "Task sleep finished successfully.");
                return null;
            }
        }
    }
}
