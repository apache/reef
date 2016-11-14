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

using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Client.Local;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Examples.Checkpointing
{
    public sealed class CheckpointMainClass
    {
        private readonly IREEFClient _reefClient;
        private readonly JobRequestBuilder _jobRequestBuilder;

        [Inject]
        private CheckpointMainClass(IREEFClient reefClient, JobRequestBuilder jobRequestBuilder)
        {
            _reefClient = reefClient;
            _jobRequestBuilder = jobRequestBuilder;
        }

        private void Run()
        {
            var driverConfiguration = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<CheckpointDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<CheckpointDriver>.Class)
                .Build();

            var jobRequest = _jobRequestBuilder
                .AddDriverConfiguration(driverConfiguration)
                .AddGlobalAssemblyForType(typeof(CheckpointDriver))
                .SetJobIdentifier("Example checkpointing job")
                .SetMaxApplicationSubmissions(1)
                .Build();

            _reefClient.Submit(jobRequest);
        }

        public static void Main()
        {
            var tang = TangFactory.GetTang();
            var localClientConfiguration = LocalRuntimeClientConfiguration.ConfigurationModule.Build();
            var injector = tang.NewInjector(localClientConfiguration);
            injector.GetInstance<CheckpointMainClass>().Run();
        }

    }
}