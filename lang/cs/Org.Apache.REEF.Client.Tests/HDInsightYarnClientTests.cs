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
using System.Net;
using System.Threading.Tasks;
using Org.Apache.REEF.Client.Yarn.RestClient;
using Org.Apache.REEF.Client.YARN.RestClient;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Client.Tests
{
    public class HDInsightYarnClientTests
    {
        [Trait("Category", "Functional")]
        [Fact(Skip = @"Requires HDInsight Cred")]
        public async Task TestGetClusterInfo()
        {
            var injector = TangFactory.GetTang().NewInjector();
            var cred = new HDInsightTestCredential();
            var urlProvider = new HDInsightRMUrlProvider();
            injector.BindVolatileInstance(GenericType<IYarnRestClientCredential>.Class, cred);
            injector.BindVolatileInstance(GenericType<IUrlProvider>.Class, urlProvider);
            var client = injector.GetInstance<IYarnRMClient>();

            var clusterInfo = await client.GetClusterInfoAsync();

            Assert.NotNull(clusterInfo);
            Assert.Equal(ClusterState.STARTED, clusterInfo.State);
            Assert.True(clusterInfo.StartedOn > 0);
        }

        [Trait("Category", "Functional")]
        [Fact(Skip = @"Requires HDInsight Cred")]
        public async Task TestGetClusterMetrics()
        {
            var injector = TangFactory.GetTang().NewInjector();
            var cred = new HDInsightTestCredential();
            var urlProvider = new HDInsightRMUrlProvider();
            injector.BindVolatileInstance(GenericType<IYarnRestClientCredential>.Class, cred);
            injector.BindVolatileInstance(GenericType<IUrlProvider>.Class, urlProvider);
            var client = injector.GetInstance<IYarnRMClient>();

            var clusterMetrics = await client.GetClusterMetricsAsync();

            Assert.NotNull(clusterMetrics);
            Assert.True(clusterMetrics.TotalMB > 0);
            Assert.True(clusterMetrics.ActiveNodes > 0);
        }

        [Trait("Category", "Functional")]
        [Fact(Skip = @"Requires HDInsight Cred")]
        public async Task TestApplicationSubmissionAndQuery()
        {
            var injector = TangFactory.GetTang().NewInjector();
            var cred = new HDInsightTestCredential();
            var urlProvider = new HDInsightRMUrlProvider();
            injector.BindVolatileInstance(GenericType<IYarnRestClientCredential>.Class, cred);
            injector.BindVolatileInstance(GenericType<IUrlProvider>.Class, urlProvider);
            var client = injector.GetInstance<IYarnRMClient>();

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
    }

    public class HDInsightTestCredential : IYarnRestClientCredential
    {
        private const string UserName = @"foo";
        private const string Password = @"bar"; // TODO: Do not checkin!!!
        private readonly ICredentials _credentials = new NetworkCredential(UserName, Password);

        public ICredentials Credentials
        {
            get { return _credentials; }
        }
    }

    public class HDInsightRMUrlProvider : IUrlProvider
    {
        private const string HDInsightUrl = "https://baz.azurehdinsight.net/";

        public Task<IEnumerable<Uri>> GetUrlAsync()
        {
            return Task.FromResult(Enumerable.Repeat(new Uri(HDInsightUrl), 1));
        }
    }
}