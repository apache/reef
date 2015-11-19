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
using System.Linq;
using System.ServiceProcess;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Client.Yarn.RestClient;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Tang.Implementations.Tang;

namespace Org.Apache.REEF.Client.Tests
{
    [TestClass]
    public class WindowsHadoopEmulatorYarnClientTests
    {
        /// <summary>
        /// TestInit here checks if the required hadoop services are running on the local machine.
        /// If the the services are not available, tests will be marked inconclusive.
        /// </summary>
        [TestInitialize]
        public void TestInit()
        {
            ServiceController[] serviceControllers = ServiceController.GetServices();
            IEnumerable<string> actualServices = serviceControllers.Select(x => x.ServiceName);

            string[] expectedServices = { "datanode", "namenode", "nodemanager", "resourcemanager" };

            bool allServicesExist = expectedServices.All(expectedService => actualServices.Contains(expectedService));

            if (!allServicesExist)
            {
                Assert.Inconclusive(
                    "At least some required windows services not installed. " +
                    "Two possible ways: HDInsight Emulator or HortonWorks Data Platform for Windows. " +
                    "Required services: " + string.Join(", ", expectedServices));
            }

            bool allServicesRunning = expectedServices.All(
                expectedService =>
                {
                    ServiceController controller = serviceControllers.First(x => x.ServiceName == expectedService);
                    return controller.Status == ServiceControllerStatus.Running;
                });

            if (!allServicesRunning)
            {
                Assert.Inconclusive("At least some required windows services are not running. " +
                                    "Required services: " + string.Join(", ", expectedServices));
            }
        }

        [TestMethod]
        [TestCategory("Functional")]
        public async Task TestGetClusterInfo()
        {
            var client = TangFactory.GetTang().NewInjector().GetInstance<IYarnRMClient>();

            var clusterInfo = await client.GetClusterInfoAsync();

            Assert.IsNotNull(clusterInfo);
            Assert.AreEqual(ClusterState.STARTED, clusterInfo.State);
            Assert.IsTrue(clusterInfo.StartedOn > 0);
        }

        [TestMethod]
        [TestCategory("Functional")]
        public async Task TestGetClusterMetrics()
        {
            var client = TangFactory.GetTang().NewInjector().GetInstance<IYarnRMClient>();

            var clusterMetrics = await client.GetClusterMetricsAsync();

            Assert.IsNotNull(clusterMetrics);
            Assert.IsTrue(clusterMetrics.TotalMB > 0);
            Assert.IsTrue(clusterMetrics.ActiveNodes > 0);
        }

        [TestMethod]
        [TestCategory("Functional")]
        public async Task TestApplicationSubmissionAndQuery()
        {
            var client = TangFactory.GetTang().NewInjector().GetInstance<IYarnRMClient>();

            var newApplication = await client.CreateNewApplicationAsync();

            Assert.IsNotNull(newApplication);
            Assert.IsFalse(string.IsNullOrEmpty(newApplication.ApplicationId));
            Assert.IsTrue(newApplication.MaximumResourceCapability.MemoryMB > 0);
            Assert.IsTrue(newApplication.MaximumResourceCapability.VCores > 0);

            string applicationName = "REEFTEST_APPLICATION_" + Guid.NewGuid();
            Console.WriteLine(applicationName);

            const string anyApplicationType = "REEFTest";
            var submitApplicationRequest = new SubmitApplication
            {
                ApplicationId = newApplication.ApplicationId,
                AmResource = new Resouce
                {
                    MemoryMB = 500,
                    VCores = 1
                },
                ApplicationType = anyApplicationType,
                ApplicationName = applicationName,
                KeepContainersAcrossApplicationAttempts = false,
                MaxAppAttempts = 1,
                Priority = 1,
                UnmanagedAM = false,
                AmContainerSpec = new AmContainerSpec
                {
                    Commands = new Commands
                    {
                        Command = @"DONTCARE"
                    },
                    LocalResources = new LocalResources
                    {
                        Entries = new List<YARN.RestClient.DataModel.KeyValuePair<string, LocalResourcesValue>>
                        {
                            new YARN.RestClient.DataModel.KeyValuePair<string, LocalResourcesValue>
                            {
                                Key = "APPLICATIONWILLFAILBUTWEDONTCAREHERE",
                                Value = new LocalResourcesValue
                                {
                                    Resource = "Foo",
                                    Type = ResourceType.FILE,
                                    Visibility = Visibility.APPLICATION
                                }
                            }
                        }
                    }
                }
            };

            var application = await client.SubmitApplicationAsync(submitApplicationRequest);

            Assert.IsNotNull(application);
            Assert.AreEqual(newApplication.ApplicationId, application.Id);
            Assert.AreEqual(applicationName, application.Name);
            Assert.AreEqual(anyApplicationType, application.ApplicationType);

            var getApplicationResult = client.GetApplicationAsync(newApplication.ApplicationId).GetAwaiter().GetResult();

            Assert.IsNotNull(getApplicationResult);
            Assert.AreEqual(newApplication.ApplicationId, getApplicationResult.Id);
            Assert.AreEqual(applicationName, getApplicationResult.Name);
            Assert.AreEqual(anyApplicationType, getApplicationResult.ApplicationType);
        }

        [TestMethod]
        [TestCategory("Functional")]
        public async Task TestErrorResponse()
        {
            const string wrongApplicationName = @"Something";

            var client = TangFactory.GetTang().NewInjector().GetInstance<IYarnRMClient>();

            try
            {
                await client.GetApplicationAsync(wrongApplicationName);
                Assert.Fail("Should throw YarnRestAPIException");
            }
            catch (AggregateException aggregateException)
            {
                Assert.IsInstanceOfType(aggregateException.GetBaseException(), typeof(YarnRestAPIException));
            }
        }
    }
}