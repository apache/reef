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

using System.Collections.Generic;
using Org.Apache.Reef.Driver.Bridge;
using Org.Apache.Reef.Driver.Defaults;
using Org.Apache.Reef.Tasks;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Util;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Org.Apache.Reef.Test
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
            IConfiguration driverConfig = DriverBridgeConfiguration.ConfigurationModule
             .Set(DriverBridgeConfiguration.OnDriverStarted, GenericType<MessageDriver>.Class)
             .Set(DriverBridgeConfiguration.OnEvaluatorAllocated, GenericType<MessageDriver>.Class)
             .Set(DriverBridgeConfiguration.OnTaskMessage, GenericType<MessageDriver>.Class)
             .Set(DriverBridgeConfiguration.OnTaskRunning, GenericType<MessageDriver>.Class)
             .Set(DriverBridgeConfiguration.OnEvaluatorRequested, GenericType<MessageDriver>.Class)
             .Set(DriverBridgeConfiguration.CustomTraceListeners, GenericType<DefaultCustomTraceListener>.Class)
             .Set(DriverBridgeConfiguration.CustomTraceLevel, Level.Info.ToString())
             .Build();

            HashSet<string> appDlls = new HashSet<string>();
            appDlls.Add(typeof(MessageDriver).Assembly.GetName().Name);
            appDlls.Add(typeof(MessageTask).Assembly.GetName().Name);

            TestRun(appDlls, driverConfig);
            ValidateSuccessForLocalRuntime(MessageDriver.NumerOfEvaluator);
        }
    }
}