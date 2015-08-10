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
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.Examples.MapperCount;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.IO.PartitionedData.Random;
using Org.Apache.REEF.Network.Examples.GroupCommunication.PipelineBroadcastReduceDriverAndTasks;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Tests.Functional;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;

namespace Org.Apache.REEF.IMRU.Tests
{
    [TestClass]
    public class PipelinedBroadcastReduceTest : ReefFunctionalTest
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
        public void TestIMRUDriverOnLocalRuntime()
        {
            const int numTasks = 9;
            TestIMRUDriver(false, numTasks);
            ValidateSuccessForLocalRuntime(numTasks);
        }

        [Ignore]
        [TestMethod]
        public void TestIMRUDriverOnYarn()
        {
            const int numMappers = 9;
            TestIMRUDriver(true, numMappers);
        }

        private void TestIMRUDriver(bool runOnYarn, int numMappers)
        {
            AvroConfigurationSerializer configurationSerializer = new AvroConfigurationSerializer();
            const string driverId = "BroadcastReduceDriver";

            var jobDefinition = new IMRUJobDefinitionBuilder()
                .SetMapFunctionConfiguration(IMRUMapConfiguration<int, int>.ConfigurationModule
                    .Set(IMRUMapConfiguration<int, int>.MapFunction, GenericType<IdentityMapFunction>.Class)
                    .Build())
                .SetUpdateFunctionConfiguration(
                    IMRUUpdateConfiguration<int, int, int>.ConfigurationModule
                        .Set(IMRUUpdateConfiguration<int, int, int>.UpdateFunction,
                            GenericType<MapperCountUpdateFunction>.Class)
                        .Build())
                .SetMapInputCodecConfiguration(IMRUCodecConfiguration<int>.ConfigurationModule
                    .Set(IMRUCodecConfiguration<int>.Codec, GenericType<IntStreamingCodec>.Class)
                    .Build())
                .SetUpdateFunctionCodecsConfiguration(IMRUCodecConfiguration<int>.ConfigurationModule
                    .Set(IMRUCodecConfiguration<int>.Codec, GenericType<IntStreamingCodec>.Class)
                    .Build())
                .SetReduceFunctionConfiguration(IMRUReduceFunctionConfiguration<int>.ConfigurationModule
                    .Set(IMRUReduceFunctionConfiguration<int>.ReduceFunction,
                        GenericType<IntSumReduceFunction>.Class)
                    .Build())
                .SetPartitionedDatasetConfiguration(
                    RandomDataConfiguration.ConfigurationModule.Set(RandomDataConfiguration.NumberOfPartitions,
                        numMappers.ToString()).Build())
                .SetJobName("MapperCount")
                .SetNumberOfMappers(numMappers)
                .Build();

            // The driver configuration contains all the needed bindings.
            var imruDriverConfiguration = TangFactory.GetTang().NewConfigurationBuilder(new[]
            {
                DriverConfiguration.ConfigurationModule
                    .Set(DriverConfiguration.OnEvaluatorAllocated,
                        GenericType<IMRUDriver<int, int, int>>.Class)
                    .Set(DriverConfiguration.OnDriverStarted,
                        GenericType<IMRUDriver<int, int, int>>.Class)
                    .Set(DriverConfiguration.OnContextActive,
                        GenericType<IMRUDriver<int, int, int>>.Class)
                    .Set(DriverConfiguration.OnTaskCompleted,
                        GenericType<IMRUDriver<int, int, int>>.Class)
                    .Build(),
                jobDefinition.PartitionedDatasetConfgiuration
            })
                .BindNamedParameter(typeof(SerializedMapConfiguration),
                    configurationSerializer.ToString(jobDefinition.MapFunctionConfiguration))
                .BindNamedParameter(typeof(SerializedUpdateConfiguration),
                    configurationSerializer.ToString(jobDefinition.UpdateFunctionConfiguration))
                .BindNamedParameter(typeof(SerializedMapInputCodecConfiguration),
                    configurationSerializer.ToString(jobDefinition.MapInputCodecConfiguration))
                .BindNamedParameter(typeof(SerializedMapInputPipelineDataConverterConfiguration),
                    configurationSerializer.ToString(jobDefinition.MapInputPipelineDataConverterConfiguration))
                .BindNamedParameter(typeof(SerializedUpdateFunctionCodecsConfiguration),
                    configurationSerializer.ToString(jobDefinition.UpdateFunctionCodecsConfiguration))
                .BindNamedParameter(typeof(SerializedMapOutputPipelineDataConverterConfiguration),
                    configurationSerializer.ToString(jobDefinition.MapOutputPipelineDataConverterConfiguration))
                .BindNamedParameter(typeof(SerializedReduceConfiguration),
                    configurationSerializer.ToString(jobDefinition.ReduceFunctionConfiguration))
                .Build();

            IConfiguration groupCommDriverConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindStringNamedParam<GroupCommConfigurationOptions.DriverId>(driverId)
                .BindStringNamedParam<GroupCommConfigurationOptions.MasterTaskId>(IMRUConstants.UpdateTaskName)
                .BindStringNamedParam<GroupCommConfigurationOptions.GroupName>(IMRUConstants.CommunicationGroupName)
                .BindIntNamedParam<GroupCommConfigurationOptions.FanOut>(
                    IMRUConstants.TreeFanout.ToString(CultureInfo.InvariantCulture)
                        .ToString(CultureInfo.InvariantCulture))
                .BindIntNamedParam<GroupCommConfigurationOptions.NumberOfTasks>(
                    (jobDefinition.NumberOfMappers + 1).ToString(CultureInfo.InvariantCulture))
                .Build();

            imruDriverConfiguration = Configurations.Merge(imruDriverConfiguration, groupCommDriverConfig);

            HashSet<string> appDlls = new HashSet<string>();
            appDlls.Add(typeof(IDriver).Assembly.GetName().Name);
            appDlls.Add(typeof(ITask).Assembly.GetName().Name);
            appDlls.Add(typeof(IMRUJobDefinition).Assembly.GetName().Name);
            appDlls.Add(typeof(INameClient).Assembly.GetName().Name);
            appDlls.Add(typeof(INetworkService<>).Assembly.GetName().Name);
            appDlls.Add(typeof (RandomDataConfiguration).Assembly.GetName().Name);

            TestRun(appDlls, imruDriverConfiguration, runOnYarn, JavaLoggingSetting.VERBOSE);
        }
    }
}
