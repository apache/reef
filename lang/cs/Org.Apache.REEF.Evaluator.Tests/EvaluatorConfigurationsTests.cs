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

using Org.Apache.REEF.Common.Runtime.Evaluator.Utils;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Evaluator.Tests
{
    public class EvaluatorConfigurationsTests
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(EvaluatorConfigurationsTests));
        private const string EvaluatorIdPrefix = "Node-";
        private const string RemoteIdPrefix = "socket://";
        private const string AppIdForTest = "REEF_LOCAL_RUNTIME";

        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public void TestEvaluatorConfigurationFile()
        {
            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
            var avroConfiguration = serializer.AvroDeserializeFromFile("evaluator.conf");

            Assert.NotNull(avroConfiguration);
            Assert.Equal(avroConfiguration.language, Language.Java.ToString());

            foreach (var b in avroConfiguration.Bindings)
            {
               Logger.Log(Level.Info, "Key = " + b.key + " Value = " + b.value); 
            }
        }

        [Fact]
        [Trait("Priority", "0")]
        [Trait("Category", "Unit")]
        public void TestDeserializationWithAlias()
        {
            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
            var avroConfiguration = serializer.AvroDeserializeFromFile("evaluator.conf");
            var language = avroConfiguration.language;
            Assert.True(language.ToString().Equals(Language.Java.ToString()));

            var classHierarchy = TangFactory.GetTang()
                .GetClassHierarchy(new string[] { typeof(ApplicationIdentifier).Assembly.GetName().Name });
            var config = serializer.FromAvro(avroConfiguration, classHierarchy);

            IInjector evaluatorInjector = TangFactory.GetTang().NewInjector(config);
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
    }
}