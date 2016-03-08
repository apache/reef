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

using System.Linq;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote.Parameters;
using Xunit;

namespace Org.Apache.REEF.Client.Tests
{
    public class JobRequestBuilderTests
    {
        /// <summary>
        /// This is to test the configurations set from the provider are added to driver configuration through jobRequestBuilder
        /// </summary>
        [Fact]
        public void TestTcpProvider()
        {
            IConfiguration tcpPortConfig = TcpPortConfigurationModule.ConfigurationModule
                .Set(TcpPortConfigurationModule.PortRangeStart, "2000")
                .Set(TcpPortConfigurationModule.PortRangeCount, "20")
                .Build();

            var injector = TangFactory.GetTang().NewInjector(tcpPortConfig);
            var jobRequestBuilder = injector.GetInstance<JobRequestBuilder>();
            var jobRequest = jobRequestBuilder.Build();
            var driverConfig = jobRequest.AppParameters.DriverConfigurations;

            var injector2 = TangFactory.GetTang().NewInjector(Configurations.Merge(driverConfig.ToArray()));
            var portStart = injector2.GetNamedInstance<TcpPortRangeStart, int>(GenericType<TcpPortRangeStart>.Class);
            var portRange = injector2.GetNamedInstance<TcpPortRangeCount, int>(GenericType<TcpPortRangeCount>.Class);
            Assert.Equal(portStart, 2000);
            Assert.Equal(portRange, 20);
        }

        /// <summary>
        /// This is to test the defaul configuration is used without provider
        /// </summary>
        [Fact]
        public void TestTcpDefaultWithoutProvider()
        {
            var injector = TangFactory.GetTang().NewInjector();
            var jobRequestBuilder = injector.GetInstance<JobRequestBuilder>();
            var jobRequest = jobRequestBuilder.Build();
            var driverConfig = jobRequest.AppParameters.DriverConfigurations;

            var injector2 = TangFactory.GetTang().NewInjector(Configurations.Merge(driverConfig.ToArray()));
            var portStart = injector2.GetNamedInstance<TcpPortRangeStart, int>(GenericType<TcpPortRangeStart>.Class);
            var portRange = injector2.GetNamedInstance<TcpPortRangeCount, int>(GenericType<TcpPortRangeCount>.Class);
            Assert.Equal(portStart, 8900);
            Assert.Equal(portRange, 1000);
        }
    }
}