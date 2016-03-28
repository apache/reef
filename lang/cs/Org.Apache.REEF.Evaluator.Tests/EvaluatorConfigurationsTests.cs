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
using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Common.Runtime.Evaluator.Utils;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Examples.HelloREEF;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Formats.AvroConfigurationDataContract;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.StreamingCodec;
using Xunit;

namespace Org.Apache.REEF.Evaluator.Tests
{
    public class EvaluatorConfigurationsTests
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(EvaluatorConfigurationsTests));
        private const string EvaluatorIdPrefix = "Node-";
        private const string ContextIdPrefix = "RootContext_";
        private const string RemoteIdPrefix = "socket://";
        private const string AppIdForTest = "REEF_LOCAL_RUNTIME";

        /// <summary>
        /// This test is to deserialize evaluator configuration for identifiers
        /// using alias if the parameter in the configuration cannot be found in the class hierarchy.
        /// </summary>
        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public void TestDeserializationIdsWithAlias()
        {
            var config = DeserializeConfigWithAlias();
            var evaluatorInjector = TangFactory.GetTang().NewInjector(config);

            string appid = evaluatorInjector.GetNamedInstance<ApplicationIdentifier, string>();
            string remoteId = evaluatorInjector.GetNamedInstance<DriverRemoteIdentifier, string>();

            string evaluatorIdentifier = evaluatorInjector.GetNamedInstance<EvaluatorIdentifier, string>();
            string rid = evaluatorInjector.GetNamedInstance<ErrorHandlerRid, string>();
            string launchId = evaluatorInjector.GetNamedInstance<LaunchId, string>();

            Assert.True(remoteId.StartsWith(RemoteIdPrefix));
            Assert.True(appid.Equals(AppIdForTest));
            Assert.True(evaluatorIdentifier.StartsWith(EvaluatorIdPrefix));
            Assert.True(rid.StartsWith(RemoteIdPrefix));
            Assert.True(launchId.Equals(AppIdForTest));
        }

        /// <summary>
        /// This test is to deserialize evaluator configuration for Evaluator, Task, Context and Service
        /// using alias if the parameter in the configuration cannot be found in the class hierarchy.
        /// </summary>
        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public void TestDeserializeEvaluatorContextServiceTaskWithAlias()
        {
            var serializer = new AvroConfigurationSerializer();
            var config = DeserializeConfigWithAlias();
            var evaluatorInjector = TangFactory.GetTang().NewInjector(config);

            var evaluatorConfigString = evaluatorInjector.GetNamedInstance<EvaluatorConfiguration, string>();
            var taskConfigString = evaluatorInjector.GetNamedInstance<InitialTaskConfiguration, string>();
            var contextConfigString = evaluatorInjector.GetNamedInstance<RootContextConfiguration, string>();
            var serviceConfigString = evaluatorInjector.GetNamedInstance<RootServiceConfiguration, string>();

            var evaluatorClassHierarchy = TangFactory.GetTang().GetClassHierarchy(new string[]
            {
                typeof(DefaultLocalHttpDriverConnection).Assembly.GetName().Name
            });

            var evaluatorConfig = serializer.FromString(evaluatorConfigString, evaluatorClassHierarchy);
            var fullEvaluatorInjector = evaluatorInjector.ForkInjector(evaluatorConfig);

            Assert.True(fullEvaluatorInjector.GetInstance<IDriverConnection>() is DefaultLocalHttpDriverConnection);

            var contextClassHierarchy = TangFactory.GetTang().GetClassHierarchy(new string[]
            {
                typeof(Common.Context.ContextConfigurationOptions.ContextIdentifier).Assembly.GetName().Name
            });

            var contextConfig = serializer.FromString(contextConfigString, contextClassHierarchy);

            var taskClassHierarchy = TangFactory.GetTang().GetClassHierarchy(new string[]
            {
                typeof(ITask).Assembly.GetName().Name,
                typeof(HelloTask).Assembly.GetName().Name
            });

            var taskConfig = serializer.FromString(taskConfigString, taskClassHierarchy);

            var serviceClassHierarchy = TangFactory.GetTang().GetClassHierarchy(new string[]
                {
                    typeof(ServiceConfiguration).Assembly.GetName().Name,
                    typeof(IStreamingCodec<>).Assembly.GetName().Name
                });
            var serviceConfig = serializer.FromString(serviceConfigString, serviceClassHierarchy);

            var contextInjector = fullEvaluatorInjector.ForkInjector(contextConfig);
            string contextId = contextInjector.GetNamedInstance<Common.Context.ContextConfigurationOptions.ContextIdentifier, string>();
            Assert.True(contextId.StartsWith(ContextIdPrefix));

            var serviceInjector = contextInjector.ForkInjector(serviceConfig);
            var service = serviceInjector.GetInstance<ContextRuntimeTests.TestService>();
            Assert.NotNull(service);

            var taskInjector = serviceInjector.ForkInjector(taskConfig);
            var taskId = taskInjector.GetNamedInstance<TaskConfigurationOptions.Identifier, string>();
            var task = taskInjector.GetInstance<ITask>();
            Assert.True(taskId.StartsWith("HelloTask"));
            Assert.True(task is HelloTask);
        }

        /// <summary>
        /// Deserialize evaluator configuration with alias
        /// </summary>
        /// <returns></returns>
        private static IConfiguration DeserializeConfigWithAlias()
        {
            var serializer = new AvroConfigurationSerializer();

            var classHierarchy = TangFactory.GetTang()
                .GetClassHierarchy(new string[] { typeof(ApplicationIdentifier).Assembly.GetName().Name });

            var avroConfiguration = EvaluatorConfig(serializer);
            return serializer.FromAvro(avroConfiguration, classHierarchy);
        }

        /// <summary>
        /// Simulate evaluator configuration generated from Java for unit testing
        /// </summary>
        /// <param name="serializer"></param>
        /// <returns></returns>
        private static AvroConfiguration EvaluatorConfig(AvroConfigurationSerializer serializer)
        {
            var configurationEntries = new HashSet<ConfigurationEntry>();

            configurationEntries.Add(
                new ConfigurationEntry("org.apache.reef.runtime.common.evaluator.parameters.ApplicationIdentifier",
                    "REEF_LOCAL_RUNTIME"));
            configurationEntries.Add(
                new ConfigurationEntry("org.apache.reef.runtime.common.evaluator.parameters.DriverRemoteIdentifier",
                    "socket://10.130.68.76:9723"));
            configurationEntries.Add(
                new ConfigurationEntry("org.apache.reef.runtime.common.evaluator.parameters.EvaluatorIdentifier",
                    "Node-2-1447450298921"));

            var evaluatorConfiguration = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(GenericType<IDriverConnection>.Class, GenericType<DefaultLocalHttpDriverConnection>.Class)
                .Build();

            var evaluatorString = serializer.ToString(evaluatorConfiguration);
            configurationEntries.Add(
                new ConfigurationEntry("org.apache.reef.runtime.common.evaluator.parameters.EvaluatorConfiguration",
                    evaluatorString));

            var taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "HelloTask")
                .Set(TaskConfiguration.Task, GenericType<HelloTask>.Class)
                .Build();
            var taskString = serializer.ToString(taskConfiguration);
            configurationEntries.Add(
                new ConfigurationEntry("org.apache.reef.runtime.common.evaluator.parameters.InitialTaskConfiguration",
                    taskString));

            var contextConfig = Common.Context.ContextConfiguration.ConfigurationModule.Set(Common.Context.ContextConfiguration.Identifier, ContextIdPrefix).Build();
            configurationEntries.Add(
                new ConfigurationEntry("org.apache.reef.runtime.common.evaluator.parameters.RootContextConfiguration",
                    serializer.ToString(contextConfig)));

            var serviceConfiguration = ServiceConfiguration.ConfigurationModule
                .Set(ServiceConfiguration.Services, GenericType<ContextRuntimeTests.TestService>.Class)
                .Build();
            configurationEntries.Add(
                new ConfigurationEntry("org.apache.reef.runtime.common.evaluator.parameters.RootServiceConfiguration",
                    serializer.ToString(serviceConfiguration)));

            configurationEntries.Add(new ConfigurationEntry("org.apache.reef.runtime.common.launch.parameters.ErrorHandlerRID",
                "socket://10.130.68.76:9723"));
            configurationEntries.Add(new ConfigurationEntry("org.apache.reef.runtime.common.launch.parameters.LaunchID",
                "REEF_LOCAL_RUNTIME"));

            return new AvroConfiguration(Language.Java.ToString(), configurationEntries);
        }
    }
}