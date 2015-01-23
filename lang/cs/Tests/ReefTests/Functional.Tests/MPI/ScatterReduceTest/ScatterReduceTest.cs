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

using Org.Apache.Reef.Common.io;
using Org.Apache.Reef.Driver;
using Org.Apache.Reef.Driver.Bridge;
using Org.Apache.Reef.IO.Network.NetworkService;
using Org.Apache.Reef.Tasks;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Implementations;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Util;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using System.Globalization;

namespace Org.Apache.Reef.Test.Functional.Tests.MPI.ScatterReduceTest
{
    [TestClass]
    public class ScatterReduceTest : ReefFunctionalTest
    {
        [TestInitialize]
        public void TestSetup()
        {
            CleanUp();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            CleanUp();
        }

        [TestMethod]
        public void TestScatterAndReduce()
        {
            int numTasks = 5;

            IConfiguration driverConfig = TangFactory.GetTang().NewConfigurationBuilder(
                DriverBridgeConfiguration.ConfigurationModule
                    .Set(DriverBridgeConfiguration.OnDriverStarted, GenericType<ScatterReduceDriver>.Class)
                    .Set(DriverBridgeConfiguration.OnEvaluatorAllocated, GenericType<ScatterReduceDriver>.Class)
                    .Set(DriverBridgeConfiguration.OnEvaluatorRequested, GenericType<ScatterReduceDriver>.Class)
                    .Set(DriverBridgeConfiguration.OnEvaluatorFailed, GenericType<ScatterReduceDriver>.Class)
                    .Set(DriverBridgeConfiguration.OnContextActive, GenericType<ScatterReduceDriver>.Class)
                    .Set(DriverBridgeConfiguration.CustomTraceLevel, Level.Info.ToString())
                    .Build())
                .BindNamedParameter<MpiTestConfig.NumEvaluators, int>(
                    GenericType<MpiTestConfig.NumEvaluators>.Class,
                    numTasks.ToString(CultureInfo.InvariantCulture))
                .Build();
                    
            HashSet<string> appDlls = new HashSet<string>();
            appDlls.Add(typeof(IDriver).Assembly.GetName().Name);
            appDlls.Add(typeof(ITask).Assembly.GetName().Name);
            appDlls.Add(typeof(ScatterReduceDriver).Assembly.GetName().Name);
            appDlls.Add(typeof(INameClient).Assembly.GetName().Name);
            appDlls.Add(typeof(INetworkService<>).Assembly.GetName().Name);

            TestRun(appDlls, driverConfig, false, JavaLoggingSetting.VERBOSE);
            ValidateSuccessForLocalRuntime(numTasks);
        }
    }
}
