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
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Examples.MachineLearning.KMeans;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.ML.KMeans
{
    [Collection("FunctionalTests")]
    public class TestKMeans : ReefFunctionalTest
    {
        private const int K = 3;
        private const int Partitions = 2;
        private const string SmallMouseDataFile = @"mouseData_small.csv";
        private const string MouseDataFile = @"mouseData.csv";

        private readonly bool _useSmallDataSet = false;
        private string _dataFile = MouseDataFile;

        public TestKMeans()
        {
            if (_useSmallDataSet)
            {
                _dataFile = SmallMouseDataFile;
            }  

            CleanUp();
            Init();
        }

        [Fact(Skip = "TODO[JIRA REEF-1183] Requires data files not present in enlistment")]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test KMeans clustering with things directly run without reef")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestKMeansOnDirectRunViaFileSystem()
        {
            int iteration = 0;
            string executionDirectory = Path.Combine(Directory.GetCurrentDirectory(), Constants.KMeansExecutionBaseDirectory, Guid.NewGuid().ToString("N").Substring(0, 4));
            List<DataVector> centroids = DataVector.ShuffleDataAndGetInitialCentriods(_dataFile, Partitions, K, executionDirectory);
            
            // initialize all tasks
            List<LegacyKMeansTask> tasks = new List<LegacyKMeansTask>();
            List<DataVector> labeledData = new List<DataVector>();
            for (int i = 0; i < Partitions; i++)
            {
                DataPartitionCache p = new DataPartitionCache(i, executionDirectory);
                tasks.Add(new LegacyKMeansTask(p, K, executionDirectory));
                labeledData.AddRange(p.DataVectors);
            }

            float loss = float.MaxValue;
            while (true)
            {
                for (int i = 0; i < Partitions; i++)
                {
                    tasks[i].CallWithWritingToFileSystem(null);
                }
                List<DataVector> newCentroids = PartialMean.AggregateTrueMeansToFileSystem(Partitions, K, executionDirectory);
                DataVector.WriteToCentroidFile(newCentroids, executionDirectory);
                centroids = newCentroids;
                float newLoss = LegacyKMeansTask.ComputeLossFunction(centroids, labeledData);
                if (newLoss > loss)
                {
                    throw new InvalidOperationException(
                        string.Format(CultureInfo.InvariantCulture, "The new loss {0} is larger than previous loss {1}, while loss function must be monotonically decreasing across iterations", newLoss, loss));
                }
                else if (newLoss.Equals(loss))
                {
                    Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "KMeans clustering has converged with a loss value of {0} at iteration {1} ", newLoss, iteration));
                    break;
                }
                else
                {
                    loss = newLoss;
                }
                iteration++;
            }
        }

        [Fact(Skip = "TODO[JIRA REEF-1183] Requires data files not present in enlistment")]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test KMeans clustering on reef local runtime with group communications")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestKMeansOnLocalRuntimeWithGroupCommunications()
        {
            IsOnLocalRuntime = true;
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfiguration(), typeof(KMeansDriverHandlers), Partitions + 1, "KMeansDriverHandlers", "local", testFolder);
            ValidateSuccessForLocalRuntime(Partitions + 1, testFolder: testFolder);
            CleanUp(testFolder);
        }

        [Fact(Skip = "TODO[JIRA REEF-1183] Requires data files not present in enlistment")]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test KMeans clustering on reef YARN runtime - one box")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestKMeansOnYarnOneBoxWithGroupCommunications()
        {
            string testFolder = DefaultRuntimeFolder + TestId + "Yarn";
            TestRun(DriverConfiguration(), typeof(KMeansDriverHandlers), Partitions + 1, "KMeansDriverHandlers", "yarn", testFolder);
            Assert.NotNull("BreakPointChecker");
        }

        private IConfiguration DriverConfiguration()
        {
            int fanOut = 2;
            int totalEvaluators = Partitions + 1;
            string Identifier = "KMeansDriverId";

            IConfiguration driverConfig = TangFactory.GetTang().NewConfigurationBuilder(
                Org.Apache.REEF.Driver.DriverConfiguration.ConfigurationModule
                    .Set(Org.Apache.REEF.Driver.DriverConfiguration.OnDriverStarted, GenericType<KMeansDriverHandlers>.Class)
                    .Set(Org.Apache.REEF.Driver.DriverConfiguration.OnEvaluatorAllocated, GenericType<KMeansDriverHandlers>.Class)
                    .Set(Org.Apache.REEF.Driver.DriverConfiguration.OnContextActive, GenericType<KMeansDriverHandlers>.Class)
                    .Set(Org.Apache.REEF.Driver.DriverConfiguration.CommandLineArguments, "DataFile:" + _dataFile)
                    .Set(Org.Apache.REEF.Driver.DriverConfiguration.CustomTraceLevel, Level.Info.ToString())
                    .Build())
                .BindIntNamedParam<NumPartitions>(Partitions.ToString())
                .Build();

            IConfiguration groupCommunicationDriverConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindStringNamedParam<GroupCommConfigurationOptions.DriverId>(Identifier)
                .BindStringNamedParam<GroupCommConfigurationOptions.MasterTaskId>(Constants.MasterTaskId)
                .BindStringNamedParam<GroupCommConfigurationOptions.GroupName>(Constants.KMeansCommunicationGroupName)
                .BindIntNamedParam<GroupCommConfigurationOptions.FanOut>(fanOut.ToString(CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture))
                .BindIntNamedParam<GroupCommConfigurationOptions.NumberOfTasks>(totalEvaluators.ToString())
                .Build();

            IConfiguration merged = Configurations.Merge(driverConfig, groupCommunicationDriverConfig);

            IConfiguration taskConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindSetEntry<DriverBridgeConfigurationOptions.SetOfAssemblies, string>(typeof(KMeansMasterTask).Assembly.GetName().Name)
                .BindSetEntry<DriverBridgeConfigurationOptions.SetOfAssemblies, string>(typeof(NameClient).Assembly.GetName().Name)
                .Build();

            return Configurations.Merge(merged, taskConfig);
        }
    }
}
