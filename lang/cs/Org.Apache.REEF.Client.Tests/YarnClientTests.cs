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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NSubstitute;
using Org.Apache.REEF.Client.Yarn.RestClient;
using Org.Apache.REEF.Client.YARN.RestClient;
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
        public async Task TestGetClusterInfo()
        {
            // arrange
            var ctx = new TestContext();
            var urlProvider = ctx.UrlProviderFake;
            var restReqExecutor = ctx.RestRequestExecutorFake;
            var anyUri = Enumerable.Repeat(new Uri("anyscheme://anypath"), 1);
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
                anyUri.First(),
                CancellationToken.None).Returns(Task.FromResult(anyClusterInfo));

            // act
            var yarnClient = ctx.GetClient();
            ClusterInfo actualClusterInfo = await yarnClient.GetClusterInfoAsync();

            // assert
            Assert.AreEqual(anyClusterInfo, actualClusterInfo);
            var unused = urlProvider.Received(1).GetUrlAsync();
        }

        [TestMethod]
        public async Task TestGetClusterMetrics()
        {
            var ctx = new TestContext();
            var urlProvider = ctx.UrlProviderFake;
            var restReqExecutor = ctx.RestRequestExecutorFake;
            var anyUri = Enumerable.Repeat(new Uri("anyscheme://anypath"), 1);
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
                anyUri.First(),
                CancellationToken.None).Returns(Task.FromResult(anyClusterMetrics));

            var yarnClient = ctx.GetClient();
            ClusterMetrics actualClusterMetrics = await yarnClient.GetClusterMetricsAsync();

            Assert.AreEqual(anyClusterMetrics, actualClusterMetrics);
            var unused = urlProvider.Received(1).GetUrlAsync();
        }

        [TestMethod]
        public async Task TestGetApplication()
        {
            var ctx = new TestContext();
            var urlProvider = ctx.UrlProviderFake;
            var restReqExecutor = ctx.RestRequestExecutorFake;
            var anyUri = Enumerable.Repeat(new Uri("anyscheme://anypath"), 1);
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
                anyUri.First(),
                CancellationToken.None).Returns(Task.FromResult(anyApplication));

            var yarnClient = ctx.GetClient();
            Application actualApplication = await yarnClient.GetApplicationAsync(applicationId);

            Assert.AreEqual(anyApplication, actualApplication);
            var unused = urlProvider.Received(1).GetUrlAsync();
        }

        [TestMethod]
        public async Task TestGetApplicationFinalStatus()
        {
            var ctx = new TestContext();
            var urlProvider = ctx.UrlProviderFake;
            var restReqExecutor = ctx.RestRequestExecutorFake;
            var anyUri = Enumerable.Repeat(new Uri("anyscheme://anypath"), 1);
            const string applicationId = "AnyApplicationId";
            urlProvider.GetUrlAsync().Returns(Task.FromResult(anyUri));
            var anyApplication = new Application
            {
                AllocatedMB = 100,
                AmHostHttpAddress = "http://anyhttpaddress",
                AmContainerLogs = "SomeLogs",
                ApplicationType = "AnyYarnApplicationType",
                State = State.FINISHED,
                FinalStatus = FinalState.SUCCEEDED,
                Name = "AnyApplicationName",
                RunningContainers = 0
            };
            restReqExecutor.ExecuteAsync<Application>(
                Arg.Is<IRestRequest>(
                    req =>
                        req.Resource == "ws/v1/cluster/apps/" + applicationId
                        && req.RootElement == "app"
                        && req.Method == Method.GET),
                anyUri.First(),
                CancellationToken.None).Returns(Task.FromResult(anyApplication));

            var yarnClient = ctx.GetClient();

            Application actualApplication = await yarnClient.GetApplicationAsync(applicationId);

            Assert.AreEqual(actualApplication.FinalStatus, FinalState.SUCCEEDED);
        }

        [TestMethod]
        public async Task TestCreateNewApplication()
        {
            var ctx = new TestContext();
            var urlProvider = ctx.UrlProviderFake;
            var restReqExecutor = ctx.RestRequestExecutorFake;
            var anyUri = Enumerable.Repeat(new Uri("anyscheme://anypath"), 1);
            const string applicationId = "AnyApplicationId";
            urlProvider.GetUrlAsync().Returns(Task.FromResult(anyUri));
            var anyNewApplication = new NewApplication
            {
                ApplicationId = applicationId
            };
            restReqExecutor.ExecuteAsync<NewApplication>(
                Arg.Is<IRestRequest>(
                    req =>
                        req.Resource == "ws/v1/cluster/apps/new-application"
                        && req.Method == Method.POST),
                anyUri.First(),
                CancellationToken.None).Returns(Task.FromResult(anyNewApplication));

            var yarnClient = ctx.GetClient();
            NewApplication actualNewApplication = await yarnClient.CreateNewApplicationAsync();

            Assert.AreEqual(anyNewApplication, actualNewApplication);
            var unused = urlProvider.Received(1).GetUrlAsync();
        }

        [TestMethod]
        public async Task TestSubmitNewApplication()
        {
            var ctx = new TestContext();
            var urlProvider = ctx.UrlProviderFake;
            var restReqExecutor = ctx.RestRequestExecutorFake;
            var anyUri = Enumerable.Repeat(new Uri("anyscheme://anypath"), 1);
            const string applicationId = "AnyApplicationId";
            const string anyApplicationType = "REEFTest";
            const string anyApplicationName = "AnyAPP";
            urlProvider.GetUrlAsync().Returns(Task.FromResult(anyUri));
            var anySubmitApplication = new SubmitApplication
            {
                ApplicationId = applicationId,
                AmResource = new Resouce
                {
                    MemoryMB = 500,
                    VCores = 1
                },
                ApplicationType = anyApplicationType,
                ApplicationName = anyApplicationName,
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

            const string expectedJson = @"{" +
                                            @"""application-id"":""AnyApplicationId""," +
                                            @"""application-name"":""AnyAPP""," +
                                            @"""queue"":null,""priority"":1," +
                                            @"""am-container-spec"":" +
                                            @"{" +
                                                @"""local-resources"":" +
                                                @"{" +
                                                    @"""entry"":" +
                                                    @"[" +
                                                        @"{" +
                                                            @"""key"":""APPLICATIONWILLFAILBUTWEDONTCAREHERE""," +
                                                            @"""value"":" +
                                                            @"{" +
                                                                @"""resource"":""Foo""," +
                                                                @"""type"":""FILE""," +
                                                                @"""visibility"":""APPLICATION""," +
                                                                @"""size"":0," +
                                                                @"""timestamp"":0" +
                                                            @"}" +
                                                        @"}" +
                                                    @"]" +
                                                @"}," + 
                                            @"""environment"":null," +
                                            @"""commands"":" +
                                            @"{" +
                                                @"""command"":""DONTCARE""" +
                                            @"}," +
                                            @"""service-data"":null," +
                                            @"""credentials"":null," +
                                            @"""application-acls"":null}," +
                                            @"""unmanaged-AM"":false," +
                                            @"""max-app-attempts"":1," +
                                            @"""resource"":" +
                                            @"{" +
                                                @"""memory"":500," +
                                                @"""vCores"":1" +
                                            @"},""application-type"":""REEFTest""," +
                                            @"""keep-containers-across-application-attempts"":false," +
                                            @"""application-tags"":null" +
                                        @"}";

            var thisApplication = new Application
            {
                AllocatedMB = 100,
                AmHostHttpAddress = "http://anyhttpaddress",
                AmContainerLogs = "SomeLogs",
                ApplicationType = "AnyYarnApplicationType",
                State = State.FINISHED,
                Name = "AnyApplicationName",
                RunningContainers = 0
            };

            var response = Substitute.For<IRestResponse>();
            response.Headers.Returns(new List<Parameter>
            {
                new Parameter
                {
                    Name = "Location",
                    Value = "http://somelocation"
                }
            });
            response.StatusCode.Returns(HttpStatusCode.Accepted);

            restReqExecutor.ExecuteAsync(
                Arg.Is<IRestRequest>(
                    req =>
                        req.Resource == "ws/v1/cluster/apps"
                        && req.Method == Method.POST
                        && req.JsonSerializer is RestJsonSerializer
                        && req.Parameters.First().Name == "application/json"
                        && IsExpectedJson(req, expectedJson)),
                anyUri.First(),
                CancellationToken.None).Returns(Task.FromResult(response));

            restReqExecutor.ExecuteAsync<Application>(
                Arg.Is<IRestRequest>(
                    req =>
                        req.Resource == "ws/v1/cluster/apps/" + applicationId
                        && req.RootElement == "app"
                        && req.Method == Method.GET),
                anyUri.First(),
                CancellationToken.None).Returns(Task.FromResult(thisApplication));

            var yarnClient = ctx.GetClient();
            Application actualApplication = await yarnClient.SubmitApplicationAsync(anySubmitApplication);

            Assert.AreEqual(thisApplication, actualApplication);
            var unused = urlProvider.Received(2).GetUrlAsync();
        }

        private static bool IsExpectedJson(IRestRequest req, string expectedJson)
        {
            return (string)req.Parameters.First().Value == expectedJson;
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