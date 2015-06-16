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

using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Examples.Tasks.HelloTask;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Formats.AvroConfigurationDataContract;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Tang.Tests.Configuration
{
    [TestClass]
    public class TestAvroConfiguration
    {
        private static IConfigurationSerializer _serializer;

        [ClassInitialize]
        public static void ClassSetup(TestContext context)
        {
            _serializer = TangFactory.GetTang().NewInjector().GetInstance<IConfigurationSerializer>();
        }

        [TestMethod]
        public void TestFromJsonString()
        {
            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.BindImplementation(GenericType<ITask>.Class, GenericType<HelloTask>.Class);
            IConfiguration conf = cb.Build();
            string jsonStr = _serializer.ToString(conf);

            IConfiguration c = _serializer.FromString(jsonStr);
            Assert.IsNotNull(c);

            string jsonStr2 = _serializer.ToString(c);

            IConfiguration c1 = _serializer.FromString(jsonStr2);
            Assert.IsNotNull(c1);
        }

        [TestMethod]
        public void TestAddFromAvro()
        {
            var conf1 = TangFactory.GetTang().NewConfigurationBuilder()
            .BindImplementation(GenericType<ITask>.Class, GenericType<HelloTask>.Class)
            .BindNamedParameter<TaskConfigurationOptions.Identifier, string>(
                GenericType<TaskConfigurationOptions.Identifier>.Class, "Hello Task")
            .Build();

            var s = (AvroConfigurationSerializer)_serializer;
            var c = s.FromAvro(s.ToAvroConfiguration(conf1));
            Assert.IsNotNull(c);
        }

        [TestMethod]
        public void TestAddFromAvroWithNull()
        {
            var c = ((AvroConfigurationSerializer)_serializer).FromAvro(null);
            Assert.IsNull(c);
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
            var a = new AvroConfiguration(b);
            return a;
        }
    }
}