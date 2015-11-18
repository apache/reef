/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tests.Functional.Bridge
{
    [TestClass]
    public sealed class TestFailedEvaluatorEventHandler : ReefFunctionalTest
    {
        private const string FailedEvaluatorMessage = "I have succeeded in seeing a failed evaluator.";

        [TestInitialize]
        public void TestSetup()
        {
            Init();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            CleanUp();
        }

        [TestMethod, Priority(1), TestCategory("FunctionalGated")]
        [Description("Test invocation of FailedEvaluatorHandler")]
        [DeploymentItem(@".")]
        [Timeout(180 * 1000)]
        public void TestFailedEvaluatorEventHandlerOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            CleanUp(testFolder);
            TestRun(DriverConfigurations(), typeof(FailedEvaluatorDriver), 1, "failedEvaluatorTest", "local", testFolder);
            ValidateSuccessForLocalRuntime(0, numberOfEvaluatorsToFail: 1, testFolder: testFolder);
            ValidateMessageSuccessfullyLogged(FailedEvaluatorMessage, testFolder);
            CleanUp(testFolder);
        }

        public IConfiguration DriverConfigurations()
        {
            var driverConfig = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<FailedEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<FailedEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorCompleted, GenericType<FailedEvaluatorDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<FailedEvaluatorDriver>.Class)
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(driverConfig)
                .BindSetEntry<DriverBridgeConfigurationOptions.SetOfAssemblies, string>(typeof(FailEvaluatorTask).Assembly.GetName().Name)
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
                    .Set(TaskConfiguration.Identifier, "1234567")
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
                throw new NotImplementedException();
            }

            public byte[] Call(byte[] memento)
            {
                Environment.Exit(1);
                return null;
            }
        }
    }
}