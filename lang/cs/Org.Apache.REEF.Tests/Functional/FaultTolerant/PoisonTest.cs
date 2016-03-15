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
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;
using System.Threading;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Common.Poison;

namespace Org.Apache.REEF.Tests.Functional.FaultTolerant
{
    [Collection("FunctionalTests")]
    public sealed class PoisonTest : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(PoisonTest));

        private const string FailedEvaluatorMessage = "I have succeeded in seeing a failed evaluator.";

        [Fact]
        [Trait("Description", "Test Poison functionality by injecting fault in context start handler.")]
        public void TestPoisonedEvaluatorStartHanlder()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
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
                .Build();
        }

        private sealed class PoisonedEvaluatorDriver :
            IObserver<IDriverStarted>,
            IObserver<IAllocatedEvaluator>,
            IObserver<IActiveContext>,
            IObserver<IFailedEvaluator>
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
                        .Set(ContextConfiguration.Identifier, "ContextID")
                        .Set(ContextConfiguration.OnContextStart, GenericType<PoisonedEventHandler<IContextStart>>.Class)
                        .Build());
            }

            public void OnNext(IActiveContext value)
            {
                value.SubmitTask(TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, "1234567")
                    .Set(TaskConfiguration.Task, GenericType<SleepTask>.Class)
                    .Build());
            }

            public void OnNext(IFailedEvaluator value)
            {
                Logger.Log(Level.Error, FailedEvaluatorMessage);
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
                Logger.Log(Level.Verbose, "Will sleep for 1 second.");
                Thread.Sleep(1000);
                return null;
            }
        }
    }
}
