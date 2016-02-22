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
using System.Globalization;
using System.IO;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Network.Examples.GroupCommunication;
using Org.Apache.REEF.Network.Examples.GroupCommunication.BroadcastReduceDriverAndTasks;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Examples.Client
{
    class BroadcastAndReduceClient
    {
        const string Local = "local";
        const string Yarn = "yarn";
        const string DefaultRuntimeFolder = "REEF_LOCAL_RUNTIME";

        public void RunBroadcastAndReduce(bool runOnYarn, int numTasks, int startingPortNo, int portRange)
        {
            const int numIterations = 10;
            const string driverId = "BroadcastReduceDriver";
            const string groupName = "BroadcastReduceGroup";
            const string masterTaskId = "MasterTask";
            const int fanOut = 2;

            IConfiguration driverConfig = TangFactory.GetTang().NewConfigurationBuilder(
                DriverConfiguration.ConfigurationModule
                    .Set(DriverConfiguration.OnDriverStarted, GenericType<BroadcastReduceDriver>.Class)
                    .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<BroadcastReduceDriver>.Class)
                    .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<BroadcastReduceDriver>.Class)
                    .Set(DriverConfiguration.OnContextActive, GenericType<BroadcastReduceDriver>.Class)
                    .Set(DriverConfiguration.CustomTraceLevel, Level.Info.ToString())
                    .Build())
                .BindNamedParameter<GroupTestConfig.NumIterations, int>(
                    GenericType<GroupTestConfig.NumIterations>.Class,
                    numIterations.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<GroupTestConfig.NumEvaluators, int>(
                    GenericType<GroupTestConfig.NumEvaluators>.Class,
                    numTasks.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<GroupTestConfig.StartingPort, int>(
                    GenericType<GroupTestConfig.StartingPort>.Class,
                    startingPortNo.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<GroupTestConfig.PortRange, int>(
                    GenericType<GroupTestConfig.PortRange>.Class,
                    portRange.ToString(CultureInfo.InvariantCulture))
                .Build();

            IConfiguration groupCommDriverConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindStringNamedParam<GroupCommConfigurationOptions.DriverId>(driverId)
                .BindStringNamedParam<GroupCommConfigurationOptions.MasterTaskId>(masterTaskId)
                .BindStringNamedParam<GroupCommConfigurationOptions.GroupName>(groupName)
                .BindIntNamedParam<GroupCommConfigurationOptions.FanOut>(fanOut.ToString(CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture))
                .BindIntNamedParam<GroupCommConfigurationOptions.NumberOfTasks>(numTasks.ToString(CultureInfo.InvariantCulture))
                .Build();

            IConfiguration merged = Configurations.Merge(driverConfig, groupCommDriverConfig);

            IConfiguration taskConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindSetEntry<DriverBridgeConfigurationOptions.SetOfAssemblies, string>(typeof(MasterTask).Assembly.GetName().Name)
                .BindSetEntry<DriverBridgeConfigurationOptions.SetOfAssemblies, string>(typeof(NameClient).Assembly.GetName().Name)
                .Build();

            merged = Configurations.Merge(merged, taskConfig);

            string runPlatform = runOnYarn ? "yarn" : "local";
            TestRun(merged, typeof(BroadcastReduceDriver), numTasks, "BroadcastReduceDriver", runPlatform);
        }

        internal static void TestRun(IConfiguration driverConfig, Type globalAssemblyType, int numberOfEvaluator, string jobIdentifier = "myDriver", string runOnYarn = "local", string runtimeFolder = DefaultRuntimeFolder)
        {
            IInjector injector = TangFactory.GetTang().NewInjector(GetRuntimeConfiguration(runOnYarn, numberOfEvaluator, runtimeFolder));
            var reefClient = injector.GetInstance<IREEFClient>();
            var jobRequestBuilder = injector.GetInstance<JobRequestBuilder>();
            var jobSubmission = jobRequestBuilder
                .AddDriverConfiguration(driverConfig)
                .AddGlobalAssemblyForType(globalAssemblyType)
                .SetJobIdentifier(jobIdentifier)
                .Build();

            reefClient.SubmitAndGetJobStatus(jobSubmission);
        }

        internal static IConfiguration GetRuntimeConfiguration(string runOnYarn, int numberOfEvaluator, string runtimeFolder)
        {
            switch (runOnYarn)
            {
                case Local:
                    var dir = Path.Combine(".", runtimeFolder);
                    return LocalRuntimeClientConfiguration.ConfigurationModule
                        .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, numberOfEvaluator.ToString())
                        .Set(LocalRuntimeClientConfiguration.RuntimeFolder, dir)
                        .Build();
                case Yarn:
                    return YARNClientConfiguration.ConfigurationModule.Build();
                default:
                    throw new Exception("Unknown runtime: " + runOnYarn);
            }
        }
    }
}
