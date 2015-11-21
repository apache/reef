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
using System.IO;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Defaults;
using Org.Apache.REEF.Examples.AllHandlers;
using Org.Apache.REEF.Examples.Tasks.HelloTask;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tests.Functional.Bridge
{
    [TestClass]
    public class TestSimpleEventHandlers : ReefFunctionalTest
    {
        [TestInitialize]
        public void TestSetup()
        {
            Init();
        }

        [TestMethod, Priority(1), TestCategory("FunctionalGated")]
        [Description("Test Hello Handler on local runtime")]
        [DeploymentItem(@".")]
        [Timeout(180 * 1000)]
        public void RunSimpleEventHandlerOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + Guid.NewGuid().ToString("N").Substring(0, 4);
            CleanUp(testFolder);
            TestRun(DriverConfigurations(), typeof(HelloSimpleEventHandlers), 2, "simpleHandler", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, testFolder: testFolder);
            ValidateMessageSuccessfullyLogged("Evaluator is assigned with 3072 MB of memory and 1 cores.", testFolder);
            CleanUp(testFolder);
        }

        public IConfiguration DriverConfigurations()
        {
            var helloDriverConfiguration = REEF.Driver.DriverConfiguration.ConfigurationModule
                .Set(REEF.Driver.DriverConfiguration.OnDriverStarted, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnEvaluatorAllocated, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnContextActive, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskMessage, GenericType<HelloTaskMessageHandler>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnEvaluatorFailed, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskCompleted, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskFailed, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnTaskRunning, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnHttpEvent, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(REEF.Driver.DriverConfiguration.OnEvaluatorCompleted, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(REEF.Driver.DriverConfiguration.CustomTraceListeners, GenericType<DefaultCustomTraceListener>.Class)
                .Set(REEF.Driver.DriverConfiguration.CustomTraceLevel, Level.Info.ToString())
                .Set(REEF.Driver.DriverConfiguration.CommandLineArguments, "submitContextAndTask")
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(helloDriverConfiguration)
                .BindSetEntry<DriverBridgeConfigurationOptions.SetOfAssemblies, string>(typeof(HelloTask).Assembly.GetName().Name)
                .BindSetEntry<DriverBridgeConfigurationOptions.SetOfAssemblies, string>(typeof(NameClient).Assembly.GetName().Name)
                .BindNamedParameter<IsRetain, bool>(GenericType<IsRetain>.Class, "false")
                .BindNamedParameter<NumberOfEvaluators, Int32>(GenericType<NumberOfEvaluators>.Class, "1")
                .Build();
        }
    }
}