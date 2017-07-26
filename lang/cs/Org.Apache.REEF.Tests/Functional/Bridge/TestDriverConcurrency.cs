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
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Bridge
{
    /// <summary>
    /// Test class containing tests related to the concurrency of the .NET driver.
    /// </summary>
    [Collection("FunctionalTests")]
    public sealed class TestDriverConcurrency : ReefFunctionalTest
    {
        private const int EvalNum = 1;
        private const int DriverWaitTimeoutMilliseconds = 60000;

        /// <summary>
        /// Check that event handlers for the driver start event and the evaluator allocated event can be run concurrently.
        /// </summary>
        [Fact]
        public void TestStartHandlerAndEvalAllocatedHandlerOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            TestRun(GetDriverConfiguration(),
                typeof(DriverConcurrencyTestDriver),
                EvalNum,
                "TestStartHandlerAndEvalAllocatedHandlerOnLocalRuntime",
                "local",
                testFolder);
            ValidateSuccessForLocalRuntime(0, testFolder: testFolder, retryCount: 90);
            CleanUp(testFolder);
        }

        private IConfiguration GetDriverConfiguration()
        {
            return DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<DriverConcurrencyTestDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<DriverConcurrencyTestDriver>.Class)
                .Build();
        }

        private sealed class DriverConcurrencyTestDriver : IObserver<IDriverStarted>, IObserver<IAllocatedEvaluator>
        {
            private static readonly Logger Logger = Logger.GetLogger(typeof(DriverConcurrencyTestDriver));
            private readonly IEvaluatorRequestor _evaluatorRequestor;
            private readonly CountdownEvent _countdownEvent;

            [Inject]
            private DriverConcurrencyTestDriver(IEvaluatorRequestor evaluatorRequestor)
            {
                _evaluatorRequestor = evaluatorRequestor;
                _countdownEvent = new CountdownEvent(EvalNum);
            }

            public void OnNext(IDriverStarted driverStarted)
            {
                Logger.Log(Level.Info, "Requesting {0} evaluators.", EvalNum);
                _evaluatorRequestor.Submit(_evaluatorRequestor.NewBuilder()
                    .SetNumber(EvalNum)
                    .Build());

                // wait until the expected number of evaluator allocated events have fired
                Assert.True(_countdownEvent.Wait(DriverWaitTimeoutMilliseconds));
            }

            public void OnNext(IAllocatedEvaluator allocatedEvaluator)
            {
                Logger.Log(Level.Info, "Trigger {0} and close {1}", _countdownEvent, allocatedEvaluator.Id);
                _countdownEvent.Signal();
                allocatedEvaluator.Dispose();
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }
        }
    }
}
