﻿// Licensed to the Apache Software Foundation (ASF) under one
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

using System.Globalization;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Network.Examples.GroupCommunication;
using Org.Apache.REEF.Network.Examples.GroupCommunication.ScatterReduceDriverAndTasks;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Group
{
    [Collection("FunctionalTests")]
    public class ScatterReduceTest : ReefFunctionalTest
    {
        [Fact]
        public void TestScatterAndReduceOnLocalRuntime()
        {
            int numTasks = 5;
            string testFolder = DefaultRuntimeFolder + TestId;
            TestScatterAndReduce(false, numTasks, testFolder);
            ValidateSuccessForLocalRuntime(numTasks, testFolder: testFolder);
            CleanUp(testFolder);
        }

        [Fact(Skip = "Requires Yarn")]
        public void TestScatterAndReduceOnYarn()
        {
            int numTasks = 5;
            string testFolder = DefaultRuntimeFolder + TestId + "Yarn";
            TestScatterAndReduce(true, numTasks, testFolder);
        }

        private void TestScatterAndReduce(bool runOnYarn, int numTasks, string testFolder)
        {
            string runPlatform = runOnYarn ? "yarn" : "local";
            TestRun(DriverConfigurations(numTasks), typeof(ScatterReduceDriver), numTasks, "ScatterReduceDriver", runPlatform, testFolder);
        }

        private IConfiguration DriverConfigurations(int numTasks)
        {
            IConfiguration driverConfig = TangFactory.GetTang().NewConfigurationBuilder(
               DriverConfiguration.ConfigurationModule
                   .Set(DriverConfiguration.OnDriverStarted, GenericType<ScatterReduceDriver>.Class)
                   .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<ScatterReduceDriver>.Class)
                   .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<ScatterReduceDriver>.Class)
                   .Set(DriverConfiguration.OnContextActive, GenericType<ScatterReduceDriver>.Class)
                   .Set(DriverConfiguration.CustomTraceLevel, Level.Info.ToString())
                   .Build())
               .BindNamedParameter<GroupTestConfig.NumEvaluators, int>(
                   GenericType<GroupTestConfig.NumEvaluators>.Class,
                   numTasks.ToString(CultureInfo.InvariantCulture))
               .Build();

            IConfiguration groupCommDriverConfig = TangFactory.GetTang().NewConfigurationBuilder()
               .BindStringNamedParam<GroupCommConfigurationOptions.DriverId>(GroupTestConstants.DriverId)
               .BindStringNamedParam<GroupCommConfigurationOptions.MasterTaskId>(GroupTestConstants.MasterTaskId)
               .BindStringNamedParam<GroupCommConfigurationOptions.GroupName>(GroupTestConstants.GroupName)
               .BindIntNamedParam<GroupCommConfigurationOptions.FanOut>(GroupTestConstants.FanOut.ToString(CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture))
               .BindIntNamedParam<GroupCommConfigurationOptions.NumberOfTasks>(numTasks.ToString())
               .Build();

            return Configurations.Merge(driverConfig, groupCommDriverConfig);
        }
    }
}
