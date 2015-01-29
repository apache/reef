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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Org.Apache.Reef.Driver.Bridge;
using Org.Apache.Reef.Driver.Defaults;
using Org.Apache.Reef.Examples.HelloCLRBridge;
using Org.Apache.Reef.Examples.HelloCLRBridge.Handlers;
using Org.Apache.Reef.Tasks;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Util;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Org.Apache.Reef.Test
{
    [TestClass]
    public class TestHelloBridgeHandlers : ReefFunctionalTest
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
            Console.WriteLine("Post test check and clean up");
            CleanUp();
        }

        [TestMethod, Priority(1), TestCategory("FunctionalGated")]
        [Description("Test Hello Handler on local runtime")]
        [DeploymentItem(@".")]
        [Timeout(180 * 1000)]
        public void RunHelloHandlerOnLocalRuntime()
        {
            IsOnLocalRuntiime = true;
            TestRun(AssembliesToCopy(), DriverConfiguration());
            ValidateSuccessForLocalRuntime(2);
            ValidateEvaluatorSetting();
        }

        public IConfiguration DriverConfiguration()
        {
            return DriverBridgeConfiguration.ConfigurationModule
                 .Set(DriverBridgeConfiguration.OnDriverStarted, GenericType<HelloStartHandler>.Class)
                 .Set(DriverBridgeConfiguration.OnEvaluatorAllocated, GenericType<HelloAllocatedEvaluatorHandler>.Class)
                 .Set(DriverBridgeConfiguration.OnEvaluatorAllocated, GenericType<AnotherHelloAllocatedEvaluatorHandler>.Class)
                 .Set(DriverBridgeConfiguration.OnContextActive, GenericType<HelloActiveContextHandler>.Class)
                 .Set(DriverBridgeConfiguration.OnTaskMessage, GenericType<HelloTaskMessageHandler>.Class)
                 .Set(DriverBridgeConfiguration.OnEvaluatorFailed, GenericType<HelloFailedEvaluatorHandler>.Class)
                 .Set(DriverBridgeConfiguration.OnTaskFailed, GenericType<HelloFailedTaskHandler>.Class)
                 .Set(DriverBridgeConfiguration.OnTaskRunning, GenericType<HelloRunningTaskHandler>.Class)
                 .Set(DriverBridgeConfiguration.OnEvaluatorRequested, GenericType<HelloEvaluatorRequestorHandler>.Class)
                 .Set(DriverBridgeConfiguration.OnHttpEvent, GenericType<HelloHttpHandler>.Class)
                 .Set(DriverBridgeConfiguration.OnEvaluatorCompleted, GenericType<HelloCompletedEvaluatorHandler>.Class)
                 .Set(DriverBridgeConfiguration.CustomTraceListeners, GenericType<DefaultCustomTraceListener>.Class)
                 .Set(DriverBridgeConfiguration.CustomTraceLevel, Level.Info.ToString())
                 .Set(DriverBridgeConfiguration.CommandLineArguments, "submitContextAndTask")
                 .Build();
        }

        public HashSet<string> AssembliesToCopy()
        {
            HashSet<string> appDlls = new HashSet<string>();
            appDlls.Add(typeof(HelloStartHandler).Assembly.GetName().Name);
            appDlls.Add(typeof(HelloTask).Assembly.GetName().Name);
            return appDlls;
        }

        private void ValidateEvaluatorSetting()
        {
            const string successIndication = "Evaluator is assigned with 512 MB of memory and 2 cores.";
            string[] lines = File.ReadAllLines(GetLogFile(_stdout));
            string[] successIndicators = lines.Where(s => s.Contains(successIndication)).ToArray();
            Assert.IsTrue(successIndicators.Count() >= 1);
        }
    }
}