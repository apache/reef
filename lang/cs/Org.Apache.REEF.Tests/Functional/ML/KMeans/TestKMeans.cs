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
using System.Globalization;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Examples.MachineLearning.KMeans;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tests.Functional.ML.KMeans
{
    [TestClass]
    public class TestKMeans : ReefFunctionalTest
    {
        private const int K = 3;
        private const int Partitions = 2;
        private const string SmallMouseDataFile = @"mouseData_small.csv";
        private const string MouseDataFile = @"mouseData.csv";

        private readonly bool _useSmallDataSet = false;
        private string _dataFile = MouseDataFile;

        [TestInitialize()]
        public void TestSetup()
        {
            if (_useSmallDataSet)
            {
                _dataFile = SmallMouseDataFile;
            }  

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
        [Description("Test KMeans clustering with things directly run without reef")]
        [DeploymentItem(@".")]
        [DeploymentItem(@"Data", ".")]
        [Ignore]
        [Timeout(180 * 1000)]
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

        [TestMethod, Priority(1), TestCategory("FunctionalGated")]
        [Description("Test KMeans clustering on reef local runtime with group communications")]
        [DeploymentItem(@".")]
        [DeploymentItem(@"Data", ".")]
        [Ignore]
        [Timeout(180 * 1000)]
        public void TestKMeansOnLocalRuntimeWithGroupCommunications()
        {
            IsOnLocalRuntiime = true;
            TestRun(AssembliesToCopy(), DriverConfiguration());
            ValidateSuccessForLocalRuntime(Partitions + 1);
        }

        [TestMethod, Priority(1), TestCategory("FunctionalGated")]
        [Description("Test KMeans clustering on reef YARN runtime - one box")]
        [DeploymentItem(@".")]
        [DeploymentItem(@"Data", ".")]
        [Timeout(180 * 1000)]
        [Ignore]    // ignored by default
        public void TestKMeansOnYarnOneBoxWithGroupCommunications()
        {
            TestRun(AssembliesToCopy(), DriverConfiguration(), runOnYarn: true);
            Assert.IsNotNull("BreakPointChecker");
        }

        private IConfiguration DriverConfiguration()
        {
            return DriverBridgeConfiguration.ConfigurationModule
                 .Set(DriverBridgeConfiguration.OnDriverStarted, GenericType<KMeansDriverHandlers>.Class)
                 .Set(DriverBridgeConfiguration.OnEvaluatorAllocated, GenericType<KMeansDriverHandlers>.Class)
                 .Set(DriverBridgeConfiguration.OnEvaluatorRequested, GenericType<KMeansDriverHandlers>.Class)
                 .Set(DriverBridgeConfiguration.OnContextActive, GenericType<KMeansDriverHandlers>.Class)
                 .Set(DriverBridgeConfiguration.CommandLineArguments, "DataFile:" + _dataFile)
                 .Set(DriverBridgeConfiguration.CustomTraceLevel, Level.Info.ToString())
                 .Build();
        }

        private HashSet<string> AssembliesToCopy()
        {
            HashSet<string> appDlls = new HashSet<string>();
            appDlls.Add(typeof(KMeansDriverHandlers).Assembly.GetName().Name);
            appDlls.Add(typeof(LegacyKMeansTask).Assembly.GetName().Name);
            appDlls.Add(typeof(INameClient).Assembly.GetName().Name);
            appDlls.Add(typeof(INetworkService<>).Assembly.GetName().Name);
            return appDlls;
        }
    }
}
