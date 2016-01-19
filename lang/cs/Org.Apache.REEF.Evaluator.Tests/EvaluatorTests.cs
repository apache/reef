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
using System.IO;
using Org.Apache.REEF.Common.Avro;
using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Examples.Tasks.ShellTask;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Formats.AvroConfigurationDataContract;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Evaluator.Tests
{
    public class EvaluatorTests
    {
        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Functional")]
        [Trait("Description", "Parse Evaluator configuration from Java, inject and execute Shell task with DIR command based on the configuration")]
        public void CanInjectAndExecuteTask()
        {
            // to enforce that shell task dll be copied to output directory.
            ShellTask tmpTask = new ShellTask("invalid");
            Assert.NotNull(tmpTask);

            string tmp = Directory.GetCurrentDirectory();
            Assert.NotNull(tmp);

            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
            AvroConfiguration avroConfiguration = serializer.AvroDeserializeFromFile("evaluator.conf");
            Assert.NotNull(avroConfiguration);

            ICsConfigurationBuilder cb = TangFactory.GetTang().NewConfigurationBuilder();
            cb.AddConfiguration(TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "Test_CLRContext_task")
                .Set(TaskConfiguration.Task, GenericType<ShellTask>.Class)
                .Build());
            cb.BindNamedParameter<ShellTask.Command, string>(GenericType<ShellTask.Command>.Class, "dir");

            IConfiguration taskConfiguration = cb.Build();

            string taskConfig = serializer.ToString(taskConfiguration);

            ITask task = null;
            TaskConfiguration config = new TaskConfiguration(taskConfig);
            Assert.NotNull(config);
            try
            {
                IInjector injector = TangFactory.GetTang().NewInjector(config.TangConfig);
                task = (ITask)injector.GetInstance(typeof(ITask));
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("unable to inject task with configuration: " + taskConfig, e);
            }

            byte[] bytes = task.Call(null);
            string result = System.Text.Encoding.Default.GetString(bytes);

            // a dir command is executed in the container directory, which includes the file "evaluator.conf"
            Assert.True(result.Contains("evaluator.conf"));
        }

        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        [Trait("Description", "Test driver information extracted from Http server")]
        public void CanExtractDriverInformation()
        {
            const string infoString = "{\"remoteId\":\"socket://10.121.136.231:14272\",\"startTime\":\"2014 08 28 10:50:32\",\"services\":[{\"serviceName\":\"NameServer\",\"serviceInfo\":\"10.121.136.231:16663\"}]}";
            AvroDriverInfo info = AvroJsonSerializer<AvroDriverInfo>.FromString(infoString);
            Assert.True(info.remoteId.Equals("socket://10.121.136.231:14272"));
            Assert.True(info.startTime.Equals("2014 08 28 10:50:32"));
            Assert.True(new DriverInformation(info.remoteId, info.startTime, info.services).NameServerId.Equals("10.121.136.231:16663"));
        }
    }
}