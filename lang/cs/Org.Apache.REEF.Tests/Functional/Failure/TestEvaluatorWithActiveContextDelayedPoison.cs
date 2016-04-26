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
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Common.Poison;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Xunit;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;

namespace Org.Apache.REEF.Tests.Functional.Failure
{
    [Collection("FunctionalTests")]
    public sealed class TestEvaluatorWithActiveContextDelayedPoison : ReefFunctionalTest
    {
        [Fact]
        [Trait("Description", "Test evaluator failure by injecting delayed fault in context start handler.")]
        public void TestPoisonedActiveContextHandlerWithDelay()
        {
            var testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurations(), typeof(PoisonedEvaluatorDriver), 1, "poisonedActiveContextWithDelayTest", "local", testFolder);
            ValidateMessageSuccessfullyLoggedForDriver(BasePoisonedEvaluatorWithActiveContextDriver.FailedEvaluatorMessage, testFolder);

            // verify that no unexpected events happened
            ValidateMessageSuccessfullyLoggedForDriver(BasePoisonedEvaluatorDriver.UnexpectedClosedContext, testFolder, 0);
            ValidateMessageSuccessfullyLoggedForDriver(BasePoisonedEvaluatorDriver.UnexpectedFailedContext, testFolder, 0);
            ValidateMessageSuccessfullyLoggedForDriver(BasePoisonedEvaluatorDriver.UnexpectedCompletedTask, testFolder, 0);
            ValidateMessageSuccessfullyLoggedForDriver(BasePoisonedEvaluatorDriver.UnexpectedFailedTask, testFolder, 0);
            CleanUp(testFolder);
        }

        private static IConfiguration DriverConfigurations()
        {
            return DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<PoisonedEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<PoisonedEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<PoisonedEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<PoisonedEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnContextClosed, GenericType<PoisonedEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnContextFailed, GenericType<PoisonedEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<PoisonedEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnTaskFailed, GenericType<PoisonedEvaluatorDriver>.Class)
                .Build();
        }

        private sealed class PoisonedEvaluatorDriver :
            BasePoisonedEvaluatorWithActiveContextDriver,
            IObserver<IActiveContext>
        {
            [Inject]
            private PoisonedEvaluatorDriver(IEvaluatorRequestor requestor) : base(requestor)
            {
            }

            public override void OnNext(IAllocatedEvaluator value)
            {
                var contextConfig = ContextConfiguration.ConfigurationModule
                    .Set(ContextConfiguration.Identifier, ContextId)
                    .Set(ContextConfiguration.OnContextStart, GenericType<PoisonedEventHandler<IContextStart>>.Class)
                    .Build();

                var poisonConfig = TangFactory.GetTang().NewConfigurationBuilder()
                    .BindIntNamedParam<CrashTimeout>("10")
                    .BindIntNamedParam<CrashMinDelay>("10")
                    .BindNamedParameter<CrashProbability, double>(GenericType<CrashProbability>.Class, "1.0")
                    .Build();

                value.SubmitContext(Configurations.Merge(contextConfig, poisonConfig));
            }

            public void OnNext(IActiveContext value)
            {
                var taskConfig = TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, TaskId)
                    .Set(TaskConfiguration.Task, GenericType<SleepTask>.Class)
                    .Set(TaskConfiguration.OnClose, GenericType<SleepTask>.Class)
                    .Build();

                value.SubmitTask(taskConfig);
            }
        }
    }
}
