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
using System.Linq;
using System.Threading;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Client.YARN;
using Org.Apache.REEF.Client.YARN.RestClient.DataModel;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Examples.HelloREEF
{
    /// <summary>
    /// A Tool that submits HelloREEFDriver for execution on YARN.
    /// </summary>
    public sealed class HelloREEFYarn
    {
        private const int ReTryCounts = 200;
        private const int SleepTime = 2000;
        private const string DefaultPortRangeStart = "2000";
        private const string DefaultPortRangeCount = "20";
        private const string TrustedApplicationTokenIdentifier = "TrustedApplicationTokenIdentifier";
        private const string SecurityTokenId = "SecurityTokenId";
        private const string SecurityTokenPwd = "SecurityTokenPwd";

        private readonly IYarnREEFClient _reefClient;
        private readonly JobRequestBuilder _jobRequestBuilder;

        private static readonly Logger Logger = Logger.GetLogger(typeof(HelloREEFYarn));

        /// <summary>
        /// List of node names for evaluators
        /// </summary>
        private readonly IList<string> _nodeNames;

        [Inject]
        private HelloREEFYarn(IYarnREEFClient reefClient, 
            JobRequestBuilder jobRequestBuilder,
            [Parameter(typeof(NodeNames))] ISet<string> nodeNames)
        {
            _reefClient = reefClient;
            _jobRequestBuilder = jobRequestBuilder;
            _nodeNames = nodeNames.ToList();
        }

        /// <summary>
        /// Runs HelloREEF using the IREEFClient passed into the constructor.
        /// </summary>
        private void Run()
        {
            // The driver configuration contains all the needed handler bindings
            var helloDriverConfiguration = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<HelloDriverYarn>.Class)
                .Set(DriverConfiguration.OnDriverStarted, GenericType<HelloDriverYarn>.Class)              
                .Build();

            var driverConfig = TangFactory.GetTang()
                .NewConfigurationBuilder(helloDriverConfiguration);

            foreach (var n in _nodeNames)
            {
                driverConfig.BindSetEntry<NodeNames, string>(GenericType<NodeNames>.Class, n);
            }
            
            // The JobSubmission contains the Driver configuration as well as the files needed on the Driver.
            var helloJobRequest = _jobRequestBuilder
                .AddDriverConfiguration(driverConfig.Build())
                .AddGlobalAssemblyForType(typeof(HelloDriverYarn))
                .SetJobIdentifier("HelloREEF")
                .AddJobSubmissionEnvVariable("key1", "value1")
                .AddJobSubmissionEnvVariable("key2", "value2")
                .SetJavaLogLevel(JavaLoggingSetting.Verbose)
                .Build();

            var result = _reefClient.SubmitAndGetJobStatus(helloJobRequest);
            LogApplicationReport();

            //// This is an example to Kill Job Application
            //// KillApplication(result.AppId);

            var state = PullFinalJobStatus(result);
            Logger.Log(Level.Info, "Application final state : {0}.", state);
        }

        /// <summary>
        /// Sample code to get application report and log.
        /// </summary>
        private void LogApplicationReport()
        {
            Logger.Log(Level.Info, "Getting Application report...");
            var apps = _reefClient.GetApplicationReports().Result;
            foreach (var r in apps)
            {
                Logger.Log(Level.Info, "Application report -- AppId {0}: {1}.", r.Key, r.Value.ToString());
            }
        }

        /// <summary>
        /// Sample code to kill Job Application.
        /// </summary>
        /// <param name="appId">Application id to kill</param>
        private void KillApplication(string appId)
        {
            if (_reefClient.KillApplication(appId).Result)
            {
                Logger.Log(Level.Info, "Application {0} is killed successfully.", appId);
            }
            else
            {
                Logger.Log(Level.Info, 
                    "Failed to kill application {0}, possible reasons are application id is invalid or application has completed.", 
                    appId);
            }
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

        /// <summary>
        /// Get runtime configuration
        /// </summary>
        /// <returns></returns>
        private static IConfiguration GetRuntimeConfiguration(string[] args)
        {
            var c = YARNClientConfiguration.ConfigurationModule
                .Set(YARNClientConfiguration.SecurityTokenKind, TrustedApplicationTokenIdentifier)
                .Set(YARNClientConfiguration.SecurityTokenService, TrustedApplicationTokenIdentifier)
                .Build();

            File.WriteAllText(SecurityTokenId, args[0]);
            File.WriteAllText(SecurityTokenPwd, args[1]);

            IConfiguration tcpPortConfig = TcpPortConfigurationModule.ConfigurationModule
                .Set(TcpPortConfigurationModule.PortRangeStart, args.Length > 2 ? args[2] : DefaultPortRangeStart)
                .Set(TcpPortConfigurationModule.PortRangeCount, args.Length > 3 ? args[3] : DefaultPortRangeCount)
                .Build();

            return Configurations.Merge(c, tcpPortConfig);
        }

        /// <summary>
        /// HelloREEF example running on YARN
        /// Usage: Org.Apache.REEF.Examples.HelloREEF TrustedApplicaitonLLQ SecurityTokenPw [portRangerStart] [portRangeCount] [nodeName1] [nodeName2]...
        /// </summary>
        /// <param name="args"></param>
        public static void MainYarn(string[] args)
        {
            var configBuilder = TangFactory.GetTang()
                .NewConfigurationBuilder(GetRuntimeConfiguration(args));

            if (args.Length > 4)
            {
                for (int i = 4; i < args.Length; i++)
                {
                    configBuilder.BindSetEntry<NodeNames, string>(GenericType<NodeNames>.Class, args[i]);
                }
            }

            TangFactory.GetTang().NewInjector(configBuilder.Build()).GetInstance<HelloREEFYarn>().Run();
        }
    }
}