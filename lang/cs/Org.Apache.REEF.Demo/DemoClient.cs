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
using Org.Apache.REEF.Demo.Driver;
using Org.Apache.REEF.Demo.Examples;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Demo
{
    public class DemoClient
    {
        private readonly IREEFClient _reefClient;
        private readonly JobRequestBuilder _jobRequestBuilder;

        [Inject]
        private DemoClient(IREEFClient reefClient, JobRequestBuilder jobRequestBuilder)
        {
            _reefClient = reefClient;
            _jobRequestBuilder = jobRequestBuilder;
        }

        private void Run()
        {
            IConfiguration driverConf = DriverConfiguration.ConfigurationModule
                .Set(DriverConfiguration.OnDriverStarted, GenericType<DemoDriver>.Class)
                .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<DataSetMaster>.Class)
                .Set(DriverConfiguration.OnContextActive, GenericType<DataSetMaster>.Class)
                .Build();

            IConfiguration addConf = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<DataSetUri, string>(GenericType<DataSetUri>.Class, @"C:\some\file\or\folder")
                .Build();

            JobRequest jobRequest = _jobRequestBuilder
                .AddDriverConfiguration(Configurations.Merge(driverConf, addConf))
                .AddGlobalAssemblyForType(typeof(DemoDriver))
                .AddGlobalAssemblyForType(typeof(DataSetMaster))
                .SetJobIdentifier("DemoREEF")
                .Build();

            _reefClient.Submit(jobRequest);
        }

        public static void Main(string[] args)
        {
            TangFactory.GetTang().NewInjector(
                LocalRuntimeClientConfiguration.ConfigurationModule
                    .Set(LocalRuntimeClientConfiguration.NumberOfEvaluators, "6")
                        .Build())
                .GetInstance<DemoClient>().Run();
        }
    }
}
