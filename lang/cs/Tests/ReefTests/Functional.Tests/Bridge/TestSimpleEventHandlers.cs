﻿/**
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
using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Defaults;
using Org.Apache.REEF.Examples.HelloCLRBridge;
using Org.Apache.REEF.Examples.HelloCLRBridge.Handlers;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Tasks;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Examples.Tasks.HelloTask;

namespace Org.Apache.REEF.Test
{
    [TestClass]
    public class TestSimpleEventHandlers : ReefFunctionalTest
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

        //[TestMethod, Priority(1), TestCategory("FunctionalGated")]
        [Description("Test Hello Handler on local runtime")]
        [DeploymentItem(@".")]
        [Timeout(180 * 1000)]
        public void RunSimpleEventHandlerOnLocalRuntime()
        {
            IsOnLocalRuntiime = true;
            TestRun(AssembliesToCopy(), DriverConfiguration());
            ValidateSuccessForLocalRuntime(2);
            ValidateEvaluatorSetting();
        }

        public IConfiguration DriverConfiguration()
        {
            return DriverBridgeConfiguration.ConfigurationModule
                .Set(DriverBridgeConfiguration.OnDriverStarted, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(DriverBridgeConfiguration.OnEvaluatorAllocated, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(DriverBridgeConfiguration.OnEvaluatorAllocated, GenericType<AnotherHelloAllocatedEvaluatorHandler>.Class)
                .Set(DriverBridgeConfiguration.OnContextActive, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(DriverBridgeConfiguration.OnTaskMessage, GenericType<HelloTaskMessageHandler>.Class)
                .Set(DriverBridgeConfiguration.OnEvaluatorFailed, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(DriverBridgeConfiguration.OnTaskCompleted, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(DriverBridgeConfiguration.OnTaskFailed, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(DriverBridgeConfiguration.OnTaskRunning, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(DriverBridgeConfiguration.OnEvaluatorRequested, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(DriverBridgeConfiguration.OnHttpEvent, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(DriverBridgeConfiguration.OnEvaluatorCompleted, GenericType<HelloSimpleEventHandlers>.Class)
                .Set(DriverBridgeConfiguration.CustomTraceListeners, GenericType<DefaultCustomTraceListener>.Class)
                .Set(DriverBridgeConfiguration.CustomTraceLevel, Level.Info.ToString())
                .Set(DriverBridgeConfiguration.CommandLineArguments, "submitContextAndTask") 
                .Set(DriverBridgeConfiguration.OnDriverRestarted, GenericType<HelloRestartHandler>.Class)
                .Set(DriverBridgeConfiguration.OnDriverReconnect, GenericType<DefaultLocalHttpDriverConnection>.Class)
                .Set(DriverBridgeConfiguration.OnDirverRestartContextActive, GenericType<HelloDriverRestartActiveContextHandler>.Class)
                .Set(DriverBridgeConfiguration.OnDriverRestartTaskRunning, GenericType<HelloDriverRestartRunningTaskHandler>.Class)
                .Build();
        }

        public HashSet<string> AssembliesToCopy()
        {
            HashSet<string> appDlls = new HashSet<string>();
            appDlls.Add(typeof(HelloSimpleEventHandlers).Assembly.GetName().Name);
            appDlls.Add(typeof(HelloTask).Assembly.GetName().Name);
            appDlls.Add(typeof(INameServer).Assembly.GetName().Name);
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