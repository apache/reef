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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NSubstitute;
using Org.Apache.REEF.Client.Yarn.RestClient;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using RestSharp;

namespace Org.Apache.REEF.Client.Tests
{
    [TestClass]
    public class YarnClientTests
    {
        [TestMethod]
        public void TestGetClusterInfo()
        {
            // arrange
            var ctx = new TestContext();
            var urlProvider = ctx.UrlProviderFake;
            var restReqExecutor = ctx.RestRequestExecutorFake;
            Uri anyUri = new Uri("anyscheme://anypath");
            urlProvider.GetUrlAsync().Returns(Task.FromResult(anyUri));
            var anyClusterInfo = new ClusterInfo
            {
                HadoopBuildVersion = "AnyBuildVersion",
                HadoopVersion = "AnyVersion",
                HadoopVersionBuiltOn = "AnyVersionBuildOn",
            };
            restReqExecutor.ExecuteAsync<ClusterInfo>(
                Arg.Is<IRestRequest>(
                    req =>
                        req.Resource == "ws/v1/cluster/info" && req.RootElement == "clusterInfo" &&
                        req.Method == Method.GET),
                anyUri,
                CancellationToken.None).Returns(Task.FromResult(anyClusterInfo));

            // act
            var yarnClient = ctx.GetClient();
            ClusterInfo actualClusterInfo = yarnClient.GetClusterInfoAsync().GetAwaiter().GetResult();

            // assert
            Assert.AreEqual(anyClusterInfo, actualClusterInfo);
            urlProvider.Received(1).GetUrlAsync();
        }

        [TestMethod]
        public void TestGetClusterMetrics()
        {
            var ctx = new TestContext();
            var urlProvider = ctx.UrlProviderFake;
            var restReqExecutor = ctx.RestRequestExecutorFake;
            Uri anyUri = new Uri("anyscheme://anypath");
            urlProvider.GetUrlAsync().Returns(Task.FromResult(anyUri));
            var anyClusterMetrics = new ClusterMetrics
            {
                ActiveNodes = 5,
                AllocatedMB = 1000,
                AllocatedVirtualCores = 10,
                AppsCompleted = 301
            };
            restReqExecutor.ExecuteAsync<ClusterMetrics>(
                Arg.Is<IRestRequest>(
                    req =>
                        req.Resource == "ws/v1/cluster/metrics" && req.RootElement == "clusterMetrics" &&
                        req.Method == Method.GET),
                anyUri,
                CancellationToken.None).Returns(Task.FromResult(anyClusterMetrics));

            var yarnClient = ctx.GetClient();
            ClusterMetrics actualClusterMetrics = yarnClient.GetClusterMetricsAsync().GetAwaiter().GetResult();

            Assert.AreEqual(anyClusterMetrics, actualClusterMetrics);
            urlProvider.Received(1).GetUrlAsync();
        }

        [TestMethod]
        public void TestGetApplication()
        {
            var ctx = new TestContext();
            var urlProvider = ctx.UrlProviderFake;
            var restReqExecutor = ctx.RestRequestExecutorFake;
            Uri anyUri = new Uri("anyscheme://anypath");
            const string applicationId = "AnyApplicationId";
            urlProvider.GetUrlAsync().Returns(Task.FromResult(anyUri));
            var anyApplication = new Application
            {
                AllocatedMB = 100,
                AmHostHttpAddress = "http://anyhttpaddress",
                AmContainerLogs = "SomeLogs",
                ApplicationType = "AnyYarnApplicationType",
                State = State.FINISHED,
                Name = "AnyApplicationName",
                RunningContainers = 0
            };
            restReqExecutor.ExecuteAsync<Application>(
                Arg.Is<IRestRequest>(
                    req =>
                        req.Resource == "ws/v1/cluster/apps/" + applicationId
                        && req.RootElement == "app"
                        && req.Method == Method.GET),
                anyUri,
                CancellationToken.None).Returns(Task.FromResult(anyApplication));

            var yarnClient = ctx.GetClient();
            Application actualApplication = yarnClient.GetApplicationAsync(applicationId).GetAwaiter().GetResult();

            Assert.AreEqual(anyApplication, actualApplication);
            urlProvider.Received(1).GetUrlAsync();
        }

        private class TestContext
        {
            public readonly IUrlProvider UrlProviderFake = Substitute.For<IUrlProvider>();
            public readonly IRestRequestExecutor RestRequestExecutorFake = Substitute.For<IRestRequestExecutor>();

            public IYarnRMClient GetClient()
            {
                var injector = TangFactory.GetTang().NewInjector();
                injector.BindVolatileInstance(GenericType<IUrlProvider>.Class, UrlProviderFake);
                injector.BindVolatileInstance(GenericType<IRestRequestExecutor>.Class, RestRequestExecutorFake);
                return injector.GetInstance<IYarnRMClient>();
            }
        }
    }
}