/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Client.Yarn;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Examples.HelloREEF
{
    /// <summary>
    /// A Tool that submits HelloREEFDriver for execution.
    /// </summary>
    public sealed class HelloREEF
    {
        private const string Local = "local";
        private const string YARN = "yarn";
        private const int DefaultPortRangeStart = 20100;
        private const int DefaultPortRangeCount = 20;
        private readonly IREEFClient _reefClient;
        private readonly JobSubmissionBuilderFactory _jobSubmissionBuilderFactory;

        [Inject]
        private HelloREEF(IREEFClient reefClient, JobSubmissionBuilderFactory jobSubmissionBuilderFactory)
        {
            _reefClient = reefClient;
            _jobSubmissionBuilderFactory = jobSubmissionBuilderFactory;
        }

        /// <summary>
        /// Runs HelloREEF using the IREEFClient passed into the constructor.
        /// </summary>
        private void Run()
        {
            // The driver configuration contains all the needed bindings.
            var helloDriverConfiguration = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<HelloDriver>.Class)
                .Set(DriverConfiguration.OnDriverStarted, GenericType<HelloDriver>.Class)
                .Build();
            // The JobSubmission contains the Driver configuration as well as the files needed on the Driver.
            var helloJobSubmission = _jobSubmissionBuilderFactory.GetJobSubmissionBuilder()
                .AddDriverConfiguration(helloDriverConfiguration)
                .AddGlobalAssemblyForType(typeof(HelloDriver))
                .SetJobIdentifier("HelloREEF")
                .Build();

            _reefClient.SubmitAndGetDriverUrl(helloJobSubmission);
        }

        /// <summary>
        /// </summary>
        /// <param name="name"></param>
        /// <param portRangeStart="portRangeStart"></param>
        /// <param portRangeCount="portRangeCount"></param>
        /// <returns></returns>
        private static IConfiguration GetRuntimeConfiguration(string name, int portRangeStart, int portRangeCount)
        {
            IConfiguration runTimeConfig = null;
            switch (name)
            {
                case Local:
                    runTimeConfig = LocalRuntimeClientConfiguration.ConfigurationModule
                        .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, "2")
                        .Build();
                    break;
                case YARN:
                    runTimeConfig = YARNClientConfiguration.ConfigurationModule.Build();
                    break;
                default:
                    throw new Exception("Unknown runtime: " + name);
            }

            IConfiguration tcpPortConfig = TcpPortConfigurationModule.ConfigurationModule
                .Set(TcpPortConfigurationModule.PortRangeStart, portRangeStart.ToString())
                .Set(TcpPortConfigurationModule.PortRangeCount, portRangeCount.ToString())
                .Build();

            return Configurations.Merge(runTimeConfig, tcpPortConfig);
        }

        public static void Main(string[] args)
        {
            string runOnYarn = Local;
            int portRangeStart = DefaultPortRangeStart;
            int portRangeCount = DefaultPortRangeCount;

            if (args != null)
            {
                if (args.Length > 0)
                {
                    runOnYarn = args[0];
                }

                if (args.Length > 1)
                {
                    portRangeStart = int.Parse(args[1]);
                }

                if (args.Length > 2)
                {
                    portRangeCount = int.Parse(args[2]);
                }
            }

            TangFactory.GetTang()
                .NewInjector(GetRuntimeConfiguration(runOnYarn, portRangeStart, portRangeCount))
                .GetInstance<HelloREEF>()
                .Run();
        }
    }
}