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

using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote.Impl;
using Xunit;

namespace Org.Apache.REEF.Network.Tests.GroupCommunication
{
    public class GroupCommuDriverTests
    {
        [Fact]
        public void TestServiceConfiguration()
        {
            string groupName = "group1";
            string masterTaskId = "task0";
            string driverId = "Driver Id";
            int numTasks = 3;
            int fanOut = 2;

            var serializer = new AvroConfigurationSerializer();

            var groupCommunicationDriver = GroupCommunicationTests.GetInstanceOfGroupCommDriver(driverId, masterTaskId,
                groupName, fanOut,
                numTasks);

            // driver side to prepar for service config
            var codecConfig = CodecConfiguration<int>.Conf
                .Set(CodecConfiguration<int>.Codec, GenericType<IntCodec>.Class)
                .Build();
            var driverServiceConfig = groupCommunicationDriver.GetServiceConfiguration();
            var serviceConfig = Configurations.Merge(driverServiceConfig, codecConfig);

            // wrap it before serializing
            var wrappedSeriveConfig = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<ServicesConfigurationOptions.ServiceConfigString, string>(
                    GenericType<ServicesConfigurationOptions.ServiceConfigString>.Class,
                    new AvroConfigurationSerializer().ToString(serviceConfig))
                .Build();
            var serviceConfigString = serializer.ToString(wrappedSeriveConfig);

            // the configuration string is received at Evaluator side
            var serviceConfig2 = new ServiceConfiguration(serviceConfigString);

            Assert.Equal(serializer.ToString(serviceConfig), serializer.ToString(serviceConfig2.TangConfig));
        }
    }
}
