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

using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Defaults;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tests.Functional.Messaging
{
    [TestClass]
    public class TestTaskMessage : ReefFunctionalTest
    {
        [TestInitialize()]
        public void TestSetup()
        {
            CleanUp();
            Init();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            CleanUp();
        }

        /// <summary>
        /// This test is to test both task message and driver message. The messages are sent 
        /// from one side and received in the corresponding handlers and verified in the test 
        /// </summary>
        [TestMethod, Priority(1), TestCategory("FunctionalGated")]
        [Description("Test task message and driver message")]
        [DeploymentItem(@".")]
        [Timeout(180 * 1000)]
        public void TestSendTaskMessage()
        {
            string testFolder = DefaultRuntimeFolder + TestNumber++;
            CleanUp(testFolder);
            TestRun(DriverConfigurations(), typeof(MessageDriver), 1, "simpleHandler", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, testFolder: testFolder);
            CleanUp(testFolder);
        }

        public IConfiguration DriverConfigurations()
        {
            IConfiguration driverConfig = DriverConfiguration.ConfigurationModule
            .Set(DriverConfiguration.OnDriverStarted, GenericType<MessageDriver>.Class)
            .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<MessageDriver>.Class)
            .Set(DriverConfiguration.OnTaskMessage, GenericType<MessageDriver>.Class)
            .Set(DriverConfiguration.OnTaskRunning, GenericType<MessageDriver>.Class)
            .Set(DriverConfiguration.CustomTraceListeners, GenericType<DefaultCustomTraceListener>.Class)
            .Set(DriverConfiguration.CustomTraceLevel, Level.Info.ToString())
            .Build();

            IConfiguration taskConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindSetEntry<DriverBridgeConfigurationOptions.SetOfAssemblies, string>(typeof(MessageTask).Assembly.GetName().Name)
                .BindSetEntry<DriverBridgeConfigurationOptions.SetOfAssemblies, string>(typeof(NameClient).Assembly.GetName().Name)
                .Build();

            return Configurations.Merge(driverConfig, taskConfig);
        }
    }
}