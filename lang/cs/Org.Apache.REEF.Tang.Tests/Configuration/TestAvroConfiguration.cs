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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Examples.Tasks.HelloTask;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Formats.AvroConfigurationDataContract;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.Configuration
{
    public class TestAvroConfiguration
    {
        [Fact]
        public void TestFromJsonString()
        {
            IConfigurationSerializer serializerImpl = (IConfigurationSerializer)TangFactory.GetTang().NewInjector().GetInstance(typeof(IConfigurationSerializer));

            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindImplementation(GenericType<ITask>.Class, GenericType<HelloTask>.Class);
            IConfiguration conf = cb.Build();
            string jsonStr = serializerImpl.ToString(conf);

            IConfiguration c = serializerImpl.FromString(jsonStr);
            Assert.NotNull(c);

            string jsonStr2 = serializerImpl.ToString(c);

            IConfiguration c1 = serializerImpl.FromString(jsonStr2);
            Assert.NotNull(c1);
        }

        private AvroConfiguration ToAvroConfiguration()
        {
            HashSet<ConfigurationEntry> b = new HashSet<ConfigurationEntry>();
            ConfigurationEntry e1 = new ConfigurationEntry();
            e1.key = "a";
            e1.value = "a1";
            ConfigurationEntry e2 = new ConfigurationEntry();
            e2.key = "b";
            e2.value = "b1=b2";
            b.Add(e1);
            b.Add(e2);
            var a = new AvroConfiguration(Language.Cs.ToString(), b);
            return a;
        }
    }
}