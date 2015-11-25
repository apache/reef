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

using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Common.Runtime.Evaluator.Utils;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Examples.HelloREEF;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Types;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Parameters;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.Evaluator.Tests
{
    [TestClass]
    public class EvaluatorConfigurationsTests
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(EvaluatorConfigurationsTests));
        private const string EvaluatorIdPrefix = "Node-";
        private const string ContextIdPrefix = "RootContext_";
        private const string RemoteIdPrefix = "socket://";
        private const string AppIdForTest = "REEF_LOCAL_RUNTIME";


        [TestMethod, Priority(0), TestCategory("Unit")]
        public void TestEvaluatorConfigurations()
        {
            EvaluatorConfigurations evaluatorConfigurations = new EvaluatorConfigurations("evaluator.conf");

            var eId = evaluatorConfigurations.EvaluatorId;
            var aId = evaluatorConfigurations.ApplicationId;
            var rId = evaluatorConfigurations.ErrorHandlerRID;

            Logger.Log(Level.Info, "EvaluatorId = " + eId);
            Logger.Log(Level.Info, "ApplicationId = " + aId);
            Logger.Log(Level.Info, "ErrorHandlerRID = " + rId);

            Assert.IsTrue(eId.StartsWith(EvaluatorIdPrefix));
            Assert.IsTrue(aId.Equals(AppIdForTest));
            Assert.IsTrue(rId.StartsWith(RemoteIdPrefix));

            var contextConfigString = evaluatorConfigurations.RootContextConfigurationString;
            var serviceConfigString = evaluatorConfigurations.RootServiceConfigurationString;
            var taskConfigString = evaluatorConfigurations.TaskConfigurationString;

            Assert.IsFalse(string.IsNullOrWhiteSpace(contextConfigString));
            Assert.IsFalse(string.IsNullOrWhiteSpace(taskConfigString));
        }

        [TestMethod, Priority(0), TestCategory("Unit")]
        [DeploymentItem(@".")]
        public void TestEvaluatorConfigurationFile()
        {
            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
            var avroConfiguration = serializer.AvroDeserializeFromFile("evaluator.conf");

            Assert.IsNotNull(avroConfiguration);
            Assert.AreEqual(avroConfiguration.language, Language.Java.ToString());

            foreach (var b in avroConfiguration.Bindings)
            {
               Logger.Log(Level.Info, "Key = " + b.key + " Value = " + b.value); 
            }
        }

        [TestMethod, Priority(0), TestCategory("Unit")]
        [DeploymentItem(@".")]
        public void TestDeserializationWithAlias()
        {
            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
            var avroConfiguration = serializer.AvroDeserializeFromFile("evaluator.conf");
            var language = avroConfiguration.language;
            Assert.IsTrue(language.ToString().Equals(Language.Java.ToString()));

            var classHierarchy = TangFactory.GetTang()
                .GetClassHierarchy(new string[] { typeof(ApplicationIdentifier).Assembly.GetName().Name });
            var config = serializer.FromAvro(avroConfiguration, classHierarchy);

            IInjector evaluatorInjector = TangFactory.GetTang().NewInjector(config);
            string appid = evaluatorInjector.GetNamedInstance<ApplicationIdentifier, string>();
            string remoteId = evaluatorInjector.GetNamedInstance<DriverRemoteIdentifier, string>();

            string evaluatorIdentifier = evaluatorInjector.GetNamedInstance<EvaluatorIdentifier, string>();
            string rid = evaluatorInjector.GetNamedInstance<ErrorHandlerRid, string>();
            string launchId = evaluatorInjector.GetNamedInstance<LaunchId, string>();

            Assert.IsTrue(remoteId.StartsWith(RemoteIdPrefix));
            Assert.IsTrue(appid.Equals(AppIdForTest));
            Assert.IsTrue(evaluatorIdentifier.StartsWith(EvaluatorIdPrefix));
            Assert.IsTrue(rid.StartsWith(RemoteIdPrefix));
            Assert.IsTrue(launchId.Equals(AppIdForTest));
        }

        /// <summary>
        /// This test is to deserialize a evaluator configuration file using alias if the parameter cannot be 
        /// found in the class hierarchy. The config file used in the test was generated when running HelloRREEF.
        /// It contains task and context configuration strings.  
        /// </summary>
        [TestMethod, Priority(0), TestCategory("Unit")]
        [DeploymentItem(@".")]
        public void TestDeserializationForContextAndTask()
        {
            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
            
            var classHierarchy = TangFactory.GetTang()
                .GetClassHierarchy(new string[] { typeof(ApplicationIdentifier).Assembly.GetName().Name });
            var config = serializer.FromFile("evaluator.conf", classHierarchy);

            IInjector evaluatorInjector = TangFactory.GetTang().NewInjector(config);

            string taskConfigString = evaluatorInjector.GetNamedInstance<InitialTaskConfiguration, string>();
            string contextConfigString = evaluatorInjector.GetNamedInstance<RootContextConfiguration, string>();

            var contextClassHierarchy = TangFactory.GetTang().GetClassHierarchy(new string[]
            {
                typeof(ContextConfigurationOptions.ContextIdentifier).Assembly.GetName().Name
            });
            var contextConfig = serializer.FromString(contextConfigString, contextClassHierarchy);

            var taskClassHierarchy = TangFactory.GetTang().GetClassHierarchy(new string[]
            {
                typeof(ITask).Assembly.GetName().Name,
                typeof(HelloTask).Assembly.GetName().Name
            });
            var taskConfig = serializer.FromString(taskConfigString, taskClassHierarchy);

            var contextInjector = evaluatorInjector.ForkInjector(contextConfig);
            string contextId = contextInjector.GetNamedInstance<ContextConfigurationOptions.ContextIdentifier, string>();
            Assert.IsTrue(contextId.StartsWith(ContextIdPrefix));

            var taskInjector = contextInjector.ForkInjector(taskConfig);

            string taskId = taskInjector.GetNamedInstance<TaskConfigurationOptions.Identifier, string>();
            ITask task = taskInjector.GetInstance<ITask>();
            Assert.IsTrue(taskId.StartsWith("HelloTask"));
            Assert.IsTrue(task is HelloTask);
        }

        /// <summary>
        /// This test is to deserialize a evaluator configuration file using alias if the parameter cannot be 
        /// found in the class hierarchy. The config file used in the test was generated when running TestBroadCastReduceOperators.
        /// It contains service and context configuration strings.  
        /// </summary>
        [TestMethod, Priority(0), TestCategory("Unit")]
        [DeploymentItem(@".")]
        public void TestDeserializationForServiceAndContext()
        {
            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();

            var classHierarchy = TangFactory.GetTang()
                .GetClassHierarchy(new string[] { typeof(ApplicationIdentifier).Assembly.GetName().Name });
            var config = serializer.FromFile("evaluatorWithService.conf", classHierarchy);

            IInjector evaluatorInjector = TangFactory.GetTang().NewInjector(config);

            string contextConfigString = evaluatorInjector.GetNamedInstance<RootContextConfiguration, string>();
            string rootServiceConfigString = evaluatorInjector.GetNamedInstance<RootServiceConfiguration, string>();

            var contextClassHierarchy = TangFactory.GetTang().GetClassHierarchy(new string[]
            {
                typeof(ContextConfigurationOptions.ContextIdentifier).Assembly.GetName().Name
            });

            var contextConfig = serializer.FromString(contextConfigString, contextClassHierarchy);

            var serviceClassHierarchy = TangFactory.GetTang().GetClassHierarchy(new string[]
            {
                typeof(ServicesConfigurationOptions).Assembly.GetName().Name,
                typeof(IStreamingCodec<>).Assembly.GetName().Name
            });
            var rootServiceConfig = serializer.FromString(rootServiceConfigString, serviceClassHierarchy);

            var contextInjector = evaluatorInjector.ForkInjector(contextConfig);
            string contextId = contextInjector.GetNamedInstance<ContextConfigurationOptions.ContextIdentifier, string>();
            Assert.IsTrue(contextId.StartsWith("MasterTaskContext"));

            string serviceConfigString = TangFactory.GetTang().NewInjector(rootServiceConfig)
                .GetNamedInstance<ServicesConfigurationOptions.ServiceConfigString, string>();

            var serviceConfig = serializer.FromString(serviceConfigString, serviceClassHierarchy);

            var serviceInjector = contextInjector.ForkInjector(serviceConfig);
            var tcpCountRange = serviceInjector.GetNamedInstance<TcpPortRangeStart, int>();
            var tcpCountCount = serviceInjector.GetNamedInstance<TcpPortRangeCount, int>();
            Assert.IsTrue(tcpCountRange > 0);
            Assert.IsTrue(tcpCountCount > 0);
        }
    }
}