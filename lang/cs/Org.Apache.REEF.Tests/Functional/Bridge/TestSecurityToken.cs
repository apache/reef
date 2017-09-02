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

using System.Collections.Generic;
using System.IO;
using System.Threading;
using Newtonsoft.Json;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.YARN;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Examples.HelloREEF;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Bridge
{
    /// <summary>
    /// Test set security tokens
    /// </summary>
    [Collection("FunctionalTests")]
    public class TestSecurityToken
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TestSecurityToken));

        private const string DefaultPortRangeStart = "2000";
        private const string DefaultPortRangeCount = "20";

        private const string Identifier1 = "TrustedApplicationTokenIdentifier";
        private const string Identifier2 = "TrustedApplicationTokenIdentifier2";
        private const string TokenKey1 = "TrustedApplication001";
        private const string TokenKey2 = "TrustedApplication002";
        private const string Password1 = "none";
        private const string Password2 = "none";

        /// <summary>
        /// This is to test pass multiple tokens from client for Yarn application
        /// </summary>
        [Fact]
        [Trait("Environment", "Yarn")]
        [Trait("Priority", "1")]
        [Trait("Description", "Run CLR Test on Yarn")]
        public void TestSetMultipleSecurityToken()
        {
            TestRun(GetRuntimeConfigurationForMultipleTokens());
        }

        /// <summary>
        /// This is to test write token in old approach for backward compatibility checking
        /// </summary>
        [Fact]
        [Trait("Environment", "Yarn")]
        [Trait("Priority", "1")]
        [Trait("Description", "Run CLR Test on Yarn")]
        public void TestSecurityTokenBackword()
        {
            TestRun(GetRuntimeConfigurationBackwardComp());
        }

        /// <summary>
        /// This is to test passing one token from client for Yarn application
        /// </summary>
        [Fact]
        [Trait("Environment", "Yarn")]
        [Trait("Priority", "1")]
        [Trait("Description", "Run CLR Test on Yarn")]
        public void TestSetOneSecurityToken()
        {
            TestRun(GetRuntimeConfigurationForSingleToken());
        }

        /// <summary>
        /// Test run for the runtime in the given injector.
        /// </summary>
        /// <param name="condig">runtime configuration.</param>
        private void TestRun(IConfiguration condig)
        {
            IInjector injector = TangFactory.GetTang().NewInjector(condig);

            var reefClient = injector.GetInstance<IREEFClient>();
            var jobRequestBuilder = injector.GetInstance<JobRequestBuilder>();

            var jobSubmission = jobRequestBuilder
                .AddDriverConfiguration(GetDriverConfig())
                .AddGlobalAssemblyForType(typeof(HelloDriver))
                .SetJobIdentifier("MyTestJob")
                .Build();

            var result = reefClient.SubmitAndGetJobStatus(jobSubmission);
            var state = PullFinalJobStatus(result);
            Logger.Log(Level.Info, "Application final state : {0}.", state);
            Assert.Equal(FinalState.SUCCEEDED, state);
        }

        /// <summary>
        /// Use HelloDriver in the test
        /// </summary>
        /// <returns></returns>
        private IConfiguration GetDriverConfig()
        {
            return DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<HelloDriver>.Class)
                .Set(DriverConfiguration.OnDriverStarted, GenericType<HelloDriver>.Class)
                .Build();
        }

        /// <summary>
        /// Pull job final status until the Job is done
        /// </summary>
        /// <param name="jobSubmitionResult"></param>
        /// <returns></returns>
        private FinalState PullFinalJobStatus(IJobSubmissionResult jobSubmitionResult)
        {
            int n = 0;
            var state = jobSubmitionResult.FinalState;
            while (state.Equals(FinalState.UNDEFINED) && n++ < 100)
            {
                Thread.Sleep(3000);
                state = jobSubmitionResult.FinalState;
            }
            return state;
        }

        /// <summary>
        /// Get runtime configuration.
        /// Bind tokens to YarnClientCnfiguration.
        /// </summary>
        /// <returns></returns>
        private static IConfiguration GetRuntimeConfigurationForMultipleTokens()
        {
            var yarnClientConfigModule = YARNClientConfiguration.ConfigurationModule;
            foreach (var t in CreateTestTokens())
            {
                yarnClientConfigModule = yarnClientConfigModule.Set(YARNClientConfiguration.SecurityTokenStr, t);
            }

            return Configurations.Merge(yarnClientConfigModule.Build(), TcpPortConfig());
        }

        /// <summary>
        /// Get runtime configuration.
        /// Bind token to YarnClientCnfiguration.
        /// </summary>
        /// <returns></returns>
        private static IConfiguration GetRuntimeConfigurationForSingleToken()
        {
            var yarnClientConfigModule = YARNClientConfiguration.ConfigurationModule
                .Set(YARNClientConfiguration.SecurityTokenStr, CreateTestToken());

            return Configurations.Merge(yarnClientConfigModule.Build(), TcpPortConfig());
        }

        /// <summary>
        /// Get runtime configuration and token with old approach
        /// </summary>
        /// <returns></returns>
        private static IConfiguration GetRuntimeConfigurationBackwardComp()
        {
            var c = YARNClientConfiguration.ConfigurationModule
                .Set(YARNClientConfiguration.SecurityTokenKind, Identifier1)
                .Set(YARNClientConfiguration.SecurityTokenService, Identifier1)
                .Build();

            File.WriteAllText("SecurityTokenId", TokenKey1);
            File.WriteAllText("SecurityTokenPwd", Password1);

            return Configurations.Merge(c, TcpPortConfig());
        }

        private static IConfiguration TcpPortConfig()
        {
            var tcpPortConfig = TcpPortConfigurationModule.ConfigurationModule
                .Set(TcpPortConfigurationModule.PortRangeStart, DefaultPortRangeStart)
                .Set(TcpPortConfigurationModule.PortRangeCount, DefaultPortRangeCount)
                .Build();
            return tcpPortConfig;
        }

        internal static IList<string> CreateTestTokens()
        {
            SecurityTokenInfo t1 = new SecurityTokenInfo(
                Identifier1,
                Identifier1,
                ByteUtilities.StringToByteArrays(TokenKey1),
                Password1);

            SecurityTokenInfo t2 = new SecurityTokenInfo(
                Identifier2,
                Identifier2,
                ByteUtilities.StringToByteArrays(TokenKey2),
                Password2);

            IList<string> serializedTokens = new List<string>();
            serializedTokens.Add(JsonConvert.SerializeObject(t1));
            serializedTokens.Add(JsonConvert.SerializeObject(t2));

            return serializedTokens;
        }

        internal static string CreateTestToken()
        {
            SecurityTokenInfo t1 = new SecurityTokenInfo(
                Identifier1,
                Identifier1,
                ByteUtilities.StringToByteArrays(TokenKey1),
                Password1);

            return JsonConvert.SerializeObject(t1);
        }
    }
}
