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

using System.Text;
using System.Threading;
using Newtonsoft.Json;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Avro.YARN;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Tests.Functional;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Performance.TestHelloREEF
{
    /// <summary>
    /// Test Hello REEF for scalability
    /// </summary>
    [Collection("FunctionalTests")]
    public class TestHelloREEFClient : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TestHelloREEFClient));

        private const int ReTryCounts = 300;
        private const int SleepTime = 2000;
        private const string DefaultPortRangeStart = "2000";
        private const string DefaultPortRangeCount = "20";
        private const string TrustedApplicationTokenIdentifier = "TrustedApplicationTokenIdentifier";

        /// <summary>
        /// Test HelloREEF on local runtime.
        /// </summary>
        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test Hello Handler on local runtime")]
        public void TestHelloREEFOnLocal()
        {
            int numberOfContainers = 5;
            int driverMemory = 1024;
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(GetRuntimeConfigurationForLocal(numberOfContainers, testFolder), driverMemory);
            CleanUp(testFolder);
        }

        /// <summary>
        /// Test HelloREEF on YARN. 
        /// The test can be modified to pass parameters through command line arguments: token password numberOfContainers
        /// e.g. TestDriver.exe TrustedApplication001 none 2000 
        /// </summary>
        [Fact]
        [Trait("Environment", "Yarn")]
        [Trait("Priority", "1")]
        [Trait("Description", "Run CLR Test on Yarn")]
        public void TestHelloREEFOnYarn()
        {
            string[] args = { "TrustedApplication001", "none", "2000" };
            TestRun(GetRuntimeConfigurationForYarn(args), 10240);
        }

        /// <summary>
        /// Test run for the runtime in the given injector.
        /// </summary>
        /// <param name="config">runtime configuration.</param>
        /// <param name="driverMemory">driver memory in MB.</param>
        private void TestRun(IConfiguration config, int driverMemory)
        {
            IInjector injector = TangFactory.GetTang().NewInjector(config);
            var jobRequestBuilder = injector.GetInstance<JobRequestBuilder>();
            var reefClient = injector.GetInstance<IREEFClient>();
            var numberOfContainers = injector.GetNamedInstance<NumberOfContainers, int>(GenericType<NumberOfContainers>.Class);

            //// The driver configuration contains all the needed handler bindings
            var helloDriverConfiguration = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<TestHelloDriver>.Class)
                .Set(DriverConfiguration.OnDriverStarted, GenericType<TestHelloDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<TestHelloDriver>.Class)
                .Set(DriverConfiguration.OnTaskFailed, GenericType<TestHelloDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<TestHelloDriver>.Class)
                .Set(DriverConfiguration.OnTaskRunning, GenericType<TestHelloDriver>.Class)
                .Set(DriverConfiguration.CustomTraceLevel, Level.Info.ToString())
                .Build();

            var driverConfig = TangFactory.GetTang()
                .NewConfigurationBuilder(helloDriverConfiguration)
                .BindIntNamedParam<NumberOfContainers>(numberOfContainers.ToString());

            // The JobSubmission contains the Driver configuration as well as the files needed on the Driver.
            var helloJobRequest = jobRequestBuilder
                .AddDriverConfiguration(driverConfig.Build())
                .AddGlobalAssemblyForType(typeof(TestHelloDriver))
                .SetJobIdentifier("TestHelloREEF")
                .SetDriverMemory(driverMemory)
                .Build();

            var result = reefClient.SubmitAndGetJobStatus(helloJobRequest);
            var state = PullFinalJobStatus(result);
            Logger.Log(Level.Info, "Application final state : {0}.", state);
            Assert.Equal(FinalState.SUCCEEDED, state);
        }

        /// <summary>
        /// Get runtime configuration
        /// </summary>
        private static IConfiguration GetRuntimeConfigurationForYarn(string[] args)
        {
            var token = new SecurityToken(
                TrustedApplicationTokenIdentifier,
                TrustedApplicationTokenIdentifier,
                ByteUtilities.StringToByteArrays(args[0]),
                Encoding.ASCII.GetBytes(args[1]));

            var clientConfig = YARNClientConfiguration.ConfigurationModule
                .Set(YARNClientConfiguration.SecurityTokenStr, JsonConvert.SerializeObject(token))
                .Build();

            var tcpPortConfig = TcpPortConfigurationModule.ConfigurationModule
                .Set(TcpPortConfigurationModule.PortRangeStart, args.Length > 3 ? args[3] : DefaultPortRangeStart)
                .Set(TcpPortConfigurationModule.PortRangeCount, args.Length > 4 ? args[4] : DefaultPortRangeCount)
                .Build();

            var c = TangFactory.GetTang().NewConfigurationBuilder()
                .BindIntNamedParam<NumberOfContainers>(args[2])
                .Build();

            return Configurations.Merge(clientConfig, tcpPortConfig, c);
        }

        private static IConfiguration GetRuntimeConfigurationForLocal(int numberOfContainers, string testFolder)
        {
            var runtimeConfig = LocalRuntimeClientConfiguration.ConfigurationModule
                .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, numberOfContainers.ToString())
                .Set(LocalRuntimeClientConfiguration.RuntimeFolder, testFolder)
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(runtimeConfig)
                .BindIntNamedParam<NumberOfContainers>(numberOfContainers.ToString())
                .Build();
        }

        /// <summary>
        /// Sample code to pull job final status until the Job is done
        /// </summary>
        /// <param name="jobSubmitionResult"></param>
        /// <returns></returns>
        private FinalState PullFinalJobStatus(IJobSubmissionResult jobSubmitionResult)
        {
            int n = 0;
            var state = jobSubmitionResult.FinalState;
            while (state.Equals(FinalState.UNDEFINED) && n++ < ReTryCounts)
            {
                Thread.Sleep(SleepTime);
                state = jobSubmitionResult.FinalState;
            }
            return state;
        }
    }
}