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
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Examples.PeerToPeer
{
    /// <summary>
    /// A Tool that submits PeerToPeerExample for execution.
    /// </summary>
    public sealed class PeerToPeer
    {
        private const string Local = "local";
        private const string YARN = "yarn";
        private const string YARNRest = "yarnrest";
        private const string HDInsight = "hdi";
        private readonly IREEFClient _reefClient;
        private readonly JobRequestBuilder _jobRequestBuilder;

        [Inject]
        private PeerToPeer(IREEFClient reefClient, JobRequestBuilder jobRequestBuilder)
        {
            _reefClient = reefClient;
            _jobRequestBuilder = jobRequestBuilder;
        }

        /// <summary>
        /// Runs TwoFriends using the IREEFClient passed into the constructor.
        /// </summary>
        private void Run()
        {
            // The driver configuration contains all the needed bindings.
            var driverConfiguration = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<PeerToPeerDriver>.Class)
                .Set(DriverConfiguration.OnDriverStarted, GenericType<PeerToPeerDriver>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<PeerToPeerDriver>.Class)
                .Build();

            // The JobSubmission contains the Driver configuration as well as the files needed on the Driver.
            var peerToPeerExampleJobRequest = _jobRequestBuilder
                .AddDriverConfiguration(driverConfiguration)
                .AddGlobalAssemblyForType(typeof(PeerToPeerDriver))
                .SetJobIdentifier("PeerToPeerExample")
                .Build();

            _reefClient.Submit(peerToPeerExampleJobRequest);
        }

        /// <summary>
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private static IConfiguration GetRuntimeConfiguration(string name)
        {
            switch (name)
            {
                case Local:
                    return LocalRuntimeClientConfiguration.ConfigurationModule
                        .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, "2")
                        .Build();
                case YARN:
                    return YARNClientConfiguration.ConfigurationModule.Build();
                default:
                    throw new Exception("Unknown runtime: " + name);
            }
        }

        public static void Main(string[] args)
        {
            var runtimeConfiguration = GetRuntimeConfiguration(args.Length > 0 ? args[0] : Local);

            TangFactory.GetTang().NewInjector(runtimeConfiguration).GetInstance<PeerToPeer>().Run();
        }
    }
}