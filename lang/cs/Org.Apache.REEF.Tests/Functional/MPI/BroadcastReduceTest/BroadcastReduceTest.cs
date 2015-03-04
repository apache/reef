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
using System.Globalization;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tests.Functional.MPI.BroadcastReduceTest
{
    [TestClass]
    public class BroadcastReduceTest : ReefFunctionalTest
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
        public void TestBroadcastAndReduce()
        {
            int numTasks = 4;

            IConfiguration driverConfig = TangFactory.GetTang().NewConfigurationBuilder(
                DriverBridgeConfiguration.ConfigurationModule
                    .Set(DriverBridgeConfiguration.OnDriverStarted, GenericType<BroadcastReduceDriver>.Class)
                    .Set(DriverBridgeConfiguration.OnEvaluatorAllocated, GenericType<BroadcastReduceDriver>.Class)
                    .Set(DriverBridgeConfiguration.OnEvaluatorRequested, GenericType<BroadcastReduceDriver>.Class)
                    .Set(DriverBridgeConfiguration.OnEvaluatorFailed, GenericType<BroadcastReduceDriver>.Class)
                    .Set(DriverBridgeConfiguration.OnContextActive, GenericType<BroadcastReduceDriver>.Class)
                    .Set(DriverBridgeConfiguration.CustomTraceLevel, Level.Info.ToString())
                    .Build())
                .BindNamedParameter<MpiTestConfig.NumIterations, int>(
                    GenericType<MpiTestConfig.NumIterations>.Class,
                    MpiTestConstants.NumIterations.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<MpiTestConfig.NumEvaluators, int>(
                    GenericType<MpiTestConfig.NumEvaluators>.Class,
                    numTasks.ToString(CultureInfo.InvariantCulture))
                .Build();

            IConfiguration mpiDriverConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindStringNamedParam<MpiConfigurationOptions.DriverId>(MpiTestConstants.DriverId)
                .BindStringNamedParam<MpiConfigurationOptions.MasterTaskId>(MpiTestConstants.MasterTaskId)
                .BindStringNamedParam<MpiConfigurationOptions.GroupName>(MpiTestConstants.GroupName)
                .BindIntNamedParam<MpiConfigurationOptions.FanOut>(MpiTestConstants.FanOut.ToString(CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture))
                .BindIntNamedParam<MpiConfigurationOptions.NumberOfTasks>(numTasks.ToString(CultureInfo.InvariantCulture))
                .Build();

            IConfiguration merged = Configurations.Merge(driverConfig, mpiDriverConfig);
                    
            HashSet<string> appDlls = new HashSet<string>();
            appDlls.Add(typeof(IDriver).Assembly.GetName().Name);
            appDlls.Add(typeof(ITask).Assembly.GetName().Name);
            appDlls.Add(typeof(BroadcastReduceDriver).Assembly.GetName().Name);
            appDlls.Add(typeof(INameClient).Assembly.GetName().Name);
            appDlls.Add(typeof(INetworkService<>).Assembly.GetName().Name);

            TestRun(appDlls, merged, false, JavaLoggingSetting.VERBOSE);
            ValidateSuccessForLocalRuntime(numTasks);
        }
    }
}
