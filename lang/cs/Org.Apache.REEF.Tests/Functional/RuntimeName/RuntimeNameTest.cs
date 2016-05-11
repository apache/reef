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

using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Defaults;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Tests.Functional.Messaging;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Driver
{
    [Collection("FunctionalTests")]
    public class RuntimeNameTest : ReefFunctionalTest
    {
        /// <summary>
        /// Validates that runtime name is propagated to c#.
        /// </summary>
        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test TestRuntimeName. Validates that runtime name is propagated to c#")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestRuntimeNameWithoutSpecifying()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurationsWithEvaluatorRequest(GenericType<EvaluatorRequestingDriver>.Class), typeof(EvaluatorRequestingDriver), 1, "EvaluatorRequestingDriver", "local", testFolder);
            ValidateMessageSuccessfullyLoggedForDriver("Runtime Name: Local", testFolder, 2);
            CleanUp(testFolder);
        }

        /// <summary>
        /// Validates that runtime name is propagated to c#.
        /// </summary>
        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test EvaluatorRequestingDriverSpecifyingRuntimeName. Validates that runtime name is propagated to c#, when specified during submission")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestRuntimeNameSpecifyingValidName()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurationsWithEvaluatorRequest(GenericType<EvaluatorRequestingDriverSpecifyingRuntimeName>.Class), typeof(EvaluatorRequestingDriverSpecifyingRuntimeName), 1, "EvaluatorRequestingDriverSpecifyingRuntimeName", "local", testFolder);
            ValidateMessageSuccessfullyLoggedForDriver("Runtime Name: Local", testFolder, 1);
            CleanUp(testFolder);
        }

        /// <summary>
        /// Validates that runtime name is propagated to c#.
        /// </summary>
        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test TestRuntimeNameSpecifyingInvalidName. Validates that exception is thrown on c# side when invalid runtime name is specified")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestRuntimeNameSpecifyingInvalidName()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurationsWithEvaluatorRequest(GenericType<EvaluatorRequestingDriverSpecifyingInvalidRuntimeName>.Class), typeof(EvaluatorRequestingDriverSpecifyingInvalidRuntimeName), 1, "EvaluatorRequestingDriverSpecifyingInvalidRunitmeName", "local", testFolder);
            ValidateMessageSuccessfullyLoggedForDriver("System.ArgumentException: Requested runtime Yarn is not in the defined runtimes list Local", testFolder, 1);
            CleanUp(testFolder);
        }

        /// <summary>
        /// Validates that runtime name is propagated to c#, when default name is specified.
        /// </summary>
        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test TestRuntimeNameSpecifyingDefaultName. Validates that runtime name is propagated to c#")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestRuntimeNameSpecifyingDefaultName()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurationsWithEvaluatorRequest(GenericType<EvaluatorRequestingDriverSpecifyingDefaultRuntimeName>.Class), typeof(EvaluatorRequestingDriverSpecifyingDefaultRuntimeName), 1, "EvaluatorRequestingDriverSpecifyingDefaultRuntimeName", "local", testFolder);
            ValidateMessageSuccessfullyLoggedForDriver("Runtime Name: Local", testFolder, 1);
            CleanUp(testFolder);
        }

        private IConfiguration DriverConfigurationsWithEvaluatorRequest<T>(GenericType<T> type)
            where T : IObserver<IAllocatedEvaluator>, IObserver<IDriverStarted>, IObserver<IRunningTask>
        {
            return DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, type)
                .Set(DriverConfiguration.OnEvaluatorAllocated, type)
                .Set(DriverConfiguration.OnTaskRunning, type)
                .Set(DriverConfiguration.CustomTraceListeners, GenericType<DefaultCustomTraceListener>.Class)
                .Set(DriverConfiguration.CustomTraceLevel, Level.Info.ToString())
                .Build();
        }
    }
}
