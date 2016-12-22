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
using Org.Apache.REEF.Examples.MachineLearning.KMeans;
using Org.Apache.REEF.Network.Group.Config;
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
        private const string DataFileNamePrefix = "KMeansInput-";
        private const double Eps = 1E-6;

        public TestKMeans()
        {
            CleanUp();
            Init();
        }

        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test KMeans clustering with things directly run without reef")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestKMeansOnDirectRunViaFileSystem()
        {
            int iteration = 0;
            string executionDirectory = Path.Combine(Directory.GetCurrentDirectory(),
                string.Join("-", Constants.KMeansExecutionBaseDirectory, Guid.NewGuid().ToString("N").Substring(0, 4)));
            string dataFilePath = GenerateDataFileAndGetPath();
            List<DataVector> centroids = DataVector.ShuffleDataAndGetInitialCentriods(dataFilePath, Partitions, K, executionDirectory);
            
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
                if (newLoss > loss + Eps)
                {
                    throw new InvalidOperationException(
                        string.Format(CultureInfo.InvariantCulture, "The new loss {0} is larger than previous loss {1}, while loss function must be monotonically decreasing across iterations", newLoss, loss));
                }
                if (newLoss > loss - Eps)
                {
                    Console.WriteLine(string.Format(CultureInfo.InvariantCulture, "KMeans clustering has converged with a loss value of {0} at iteration {1} ", newLoss, iteration));
                    break;
                }
                loss = newLoss;
                iteration++;
            }

            // cleanup workspace
            try
            {
                Directory.Delete(executionDirectory, true);
            }
            catch (Exception)
            {
                // do not fail if clean up is unsuccessful
            }
            try
            {
                File.Delete(dataFilePath);
            }
            catch (Exception)
            {
                // do not fail if clean up is unsuccessful
            }
        }

        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test KMeans clustering on reef local runtime with group communications")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestKMeansOnLocalRuntimeWithGroupCommunications()
        {
            IsOnLocalRuntime = true;
            string testFolder = DefaultRuntimeFolder + TestId;
            string dataFilePath = GenerateDataFileAndGetPath();

            TestRun(DriverConfiguration(dataFilePath), typeof(KMeansDriverHandlers), Partitions + 1, "KMeansDriverHandlers", "local", testFolder);
            ValidateSuccessForLocalRuntime(Partitions + 1, testFolder: testFolder);
            CleanUp(testFolder);
            try
            {
                File.Delete(dataFilePath);
            }
            catch (Exception)
            {
                // do not fail if clean up is unsuccessful
            }
        }

        [Fact(Skip = "Requires Yarn Single Node")]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test KMeans clustering on reef YARN runtime - one box")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestKMeansOnYarnOneBoxWithGroupCommunications()
        {
            string testFolder = DefaultRuntimeFolder + TestId + "Yarn";
            string dataFilePath = GenerateDataFileAndGetPath();
            TestRun(DriverConfiguration(dataFilePath), typeof(KMeansDriverHandlers), Partitions + 1, "KMeansDriverHandlers", "yarn", testFolder);
            Assert.NotNull("BreakPointChecker");
        }

        private IConfiguration DriverConfiguration(string dataFilePath)
        {
            int fanOut = 2;
            int totalEvaluators = Partitions + 1;
            string identifier = "KMeansDriverId";

            IConfiguration driverConfig = TangFactory.GetTang().NewConfigurationBuilder(
                Org.Apache.REEF.Driver.DriverConfiguration.ConfigurationModule
                    .Set(Org.Apache.REEF.Driver.DriverConfiguration.OnDriverStarted, GenericType<KMeansDriverHandlers>.Class)
                    .Set(Org.Apache.REEF.Driver.DriverConfiguration.OnEvaluatorAllocated, GenericType<KMeansDriverHandlers>.Class)
                    .Set(Org.Apache.REEF.Driver.DriverConfiguration.OnContextActive, GenericType<KMeansDriverHandlers>.Class)
                    .Set(Org.Apache.REEF.Driver.DriverConfiguration.CommandLineArguments, dataFilePath)
                    .Set(Org.Apache.REEF.Driver.DriverConfiguration.CustomTraceLevel, Level.Info.ToString())
                    .Build())
                .BindIntNamedParam<NumPartitions>(Partitions.ToString())
                .Build();

            IConfiguration groupCommunicationDriverConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindStringNamedParam<GroupCommConfigurationOptions.DriverId>(identifier)
                .BindStringNamedParam<GroupCommConfigurationOptions.MasterTaskId>(Constants.MasterTaskId)
                .BindStringNamedParam<GroupCommConfigurationOptions.GroupName>(Constants.KMeansCommunicationGroupName)
                .BindIntNamedParam<GroupCommConfigurationOptions.FanOut>(fanOut.ToString(CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture))
                .BindIntNamedParam<GroupCommConfigurationOptions.NumberOfTasks>(totalEvaluators.ToString())
                .Build();

            return Configurations.Merge(driverConfig, groupCommunicationDriverConfig);
        }

        private static string GenerateDataFileAndGetPath()
        {
            string dataFilePath = Path.Combine(Path.GetTempPath(), DataFileNamePrefix + Guid.NewGuid().ToString("N").Substring(0, 4));

            DataVector[] centroids =
            {
                new DataVector(new List<float> { +2f, 0f }, 0),
                new DataVector(new List<float> { -2f, 0f }, 0),
                new DataVector(new List<float> { +0f, 2f }, 0)
            };

            using (StreamWriter writer = new StreamWriter(File.OpenWrite(dataFilePath)))
            {
                Random rnd = new Random();
                for (int i = 0; i < 10000; i++)
                {
                    int label = rnd.Next(centroids.Length);
                    float x = Convert.ToSingle(centroids[label].Data[0] + rnd.NextDouble());
                    float y = Convert.ToSingle(centroids[label].Data[1] + rnd.NextDouble());

                    writer.WriteLine("{0},{1};{2}", x, y, label);
                }
                writer.Close();
            }

            return dataFilePath;
        }
    }
}
