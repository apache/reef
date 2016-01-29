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
using Org.Apache.REEF.Client.Yarn.RestClient;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.Client.Tests
{
    // placed in the same collection as *UrlProviderTests to avoid running in parallel with them
    // as they rely on setting the same environment variable to different values
    [Collection("UrlProviderTests")]
    public class WindowsHadoopEmulatorYarnClientTests
    {
        /// <summary>
        /// This attribute checks if the required hadoop services are running on the local machine.
        /// If the the services are not available, tests annotated with this attribute will be skipped.
        /// </summary>
        class IgnoreOnHadoopNotAvailableFactAttribute : FactAttribute
        {
            public IgnoreOnHadoopNotAvailableFactAttribute()
            {
                ServiceController[] serviceControllers = ServiceController.GetServices();
                IEnumerable<string> actualServices = serviceControllers.Select(x => x.ServiceName);

                string[] expectedServices = { "datanode", "namenode", "nodemanager", "resourcemanager" };

                bool allServicesExist = expectedServices.All(expectedService => actualServices.Contains(expectedService));

                if (!allServicesExist)
                {
                    Skip =
                        "At least some required windows services not installed. " +
                        "Two possible ways: HDInsight Emulator or HortonWorks Data Platform for Windows. " +
                        "Required services: " + string.Join(", ", expectedServices);
                    return;
                }

                bool allServicesRunning = expectedServices.All(
                    expectedService =>
                    {
                        ServiceController controller = serviceControllers.First(x => x.ServiceName == expectedService);
                        return controller.Status == ServiceControllerStatus.Running;
                    });

                if (!allServicesRunning)
                {
                    Skip = "At least some required windows services are not running. " +
                           "Required services: " + string.Join(", ", expectedServices);
                }
            }
        }

        [IgnoreOnHadoopNotAvailableFact]
        [Trait("Category", "Functional")]
        public async Task TestGetClusterInfo()
        {
            var client = TangFactory.GetTang().NewInjector().GetInstance<IYarnRMClient>();

            var clusterInfo = await client.GetClusterInfoAsync();

            Assert.NotNull(clusterInfo);
            Assert.Equal(ClusterState.STARTED, clusterInfo.State);
            Assert.True(clusterInfo.StartedOn > 0);
        }

        [IgnoreOnHadoopNotAvailableFact]
        [Trait("Category", "Functional")]
        public async Task TestGetClusterMetrics()
        {
            var client = TangFactory.GetTang().NewInjector().GetInstance<IYarnRMClient>();

            var clusterMetrics = await client.GetClusterMetricsAsync();

            Assert.NotNull(clusterMetrics);
            Assert.True(clusterMetrics.TotalMB > 0);
            Assert.True(clusterMetrics.ActiveNodes > 0);
        }

        [IgnoreOnHadoopNotAvailableFact]
        [Trait("Category", "Functional")]
        public async Task TestApplicationSubmissionAndQuery()
        {
            var client = TangFactory.GetTang().NewInjector().GetInstance<IYarnRMClient>();

            var newApplication = await client.CreateNewApplicationAsync();

            Assert.NotNull(newApplication);
            Assert.False(string.IsNullOrEmpty(newApplication.ApplicationId));
            Assert.True(newApplication.MaximumResourceCapability.MemoryMB > 0);
            Assert.True(newApplication.MaximumResourceCapability.VCores > 0);

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

            Assert.NotNull(application);
            Assert.Equal(newApplication.ApplicationId, application.Id);
            Assert.Equal(applicationName, application.Name);
            Assert.Equal(anyApplicationType, application.ApplicationType);

            var getApplicationResult = client.GetApplicationAsync(newApplication.ApplicationId).GetAwaiter().GetResult();

            Assert.NotNull(getApplicationResult);
            Assert.Equal(newApplication.ApplicationId, getApplicationResult.Id);
            Assert.Equal(applicationName, getApplicationResult.Name);
            Assert.Equal(anyApplicationType, getApplicationResult.ApplicationType);
        }

        [IgnoreOnHadoopNotAvailableFact]
        [Trait("Category", "Functional")]
        public async Task TestErrorResponse()
        {
            const string wrongApplicationName = @"Something";

            var client = TangFactory.GetTang().NewInjector().GetInstance<IYarnRMClient>();

            try
            {
                await client.GetApplicationAsync(wrongApplicationName);
                Assert.True(false, "Should throw YarnRestAPIException");
            }
            catch (AggregateException aggregateException)
            {
                Assert.IsType(typeof(YarnRestAPIException), aggregateException.GetBaseException());
            }
        }
    }
}