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
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Common.Evaluator.DriverConnectionConfigurationProviders;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Examples.DriverRestart
{
    /// <summary>
    /// A REEF example that restarts the driver while having its Evaluators preserved.
    /// IMPORTANT: Can only be run on HDInsight clusters with 
    /// yarn.resourcemanager.am.max-attempts set to greater than or equal to 2.
    /// </summary>
    public sealed class DriverRestart
    {
        private readonly IREEFClient _reefClient;
        private readonly JobRequestBuilder _jobRequestBuilder;

        [Inject]
        private DriverRestart(IREEFClient reefClient, JobRequestBuilder jobRequestBuilder)
        {
            _reefClient = reefClient;
            _jobRequestBuilder = jobRequestBuilder;
        }

        /// <summary>
        /// Runs DriverRestart using the IREEFClient passed into the constructor.
        /// </summary>
        private void Run()
        {
            // The driver configuration contains all the needed bindings.
            var driverConfiguration = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<HelloRestartDriver>.Class)
                .Set(DriverConfiguration.OnDriverRestarted, GenericType<HelloRestartDriver>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<HelloRestartDriver>.Class)
                .Set(DriverConfiguration.OnTaskRunning, GenericType<HelloRestartDriver>.Class)
                .Set(DriverConfiguration.OnTaskFailed, GenericType<HelloRestartDriver>.Class)
                .Set(DriverConfiguration.OnTaskCompleted, GenericType<HelloRestartDriver>.Class)
                .Set(DriverConfiguration.OnDriverRestartCompleted, GenericType<HelloRestartDriver>.Class)
                .Set(DriverConfiguration.OnDriverRestartContextActive, GenericType<HelloRestartDriver>.Class)
                .Set(DriverConfiguration.OnDriverRestartTaskRunning, GenericType<HelloRestartDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<HelloRestartDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorFailed, GenericType<HelloRestartDriver>.Class)
                .Set(DriverConfiguration.OnDriverRestartEvaluatorFailed, GenericType<HelloRestartDriver>.Class)
                .Set(DriverConfiguration.DriverReconnectionConfigurationProvider, 
                    GenericType<YarnClusterHttpDriverReconnConfigProvider>.Class)
                .Set(DriverConfiguration.DriverRestartEvaluatorRecoverySeconds, (5 * 60).ToString())
                .Build();

            // The JobSubmission contains the Driver configuration as well as the files needed on the Driver.
            var restartJobSubmission = _jobRequestBuilder
                .AddDriverConfiguration(driverConfiguration)
                .AddGlobalAssemblyForType(typeof(HelloRestartDriver))
                .SetJobIdentifier("DriverRestart_" + Guid.NewGuid().ToString().Substring(0, 6))
                .SetMaxApplicationSubmissions(2)
                .Build();

            _reefClient.SubmitAndGetJobStatus(restartJobSubmission);
        }

        public static void Main(string[] args)
        {
            TangFactory.GetTang().NewInjector(YARNClientConfiguration.ConfigurationModule.Build()).GetInstance<DriverRestart>().Run();
        }
    }
}
