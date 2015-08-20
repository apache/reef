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
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Evaluator.Tests
{
    [TestClass]
    public class EvaluatorConfigurationsTests
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(EvaluatorConfigurationsTests));


        [TestMethod, Priority(0), TestCategory("Unit")]
        [DeploymentItem(@"ConfigFiles")]
        public void TestEvaluatorConfigurations()
        {
            EvaluatorConfigurations evaluatorConfigurations = new EvaluatorConfigurations("evaluator.conf");

            var eId = evaluatorConfigurations.EvaluatorId;
            var aId = evaluatorConfigurations.ApplicationId;
            var rId = evaluatorConfigurations.ErrorHandlerRID;

            Logger.Log(Level.Info, "EvaluatorId = " + eId);
            Logger.Log(Level.Info, "ApplicationId = " + aId);
            Logger.Log(Level.Info, "ErrorHandlerRID = " + rId);

            Assert.IsTrue(eId.Equals("Node-1-1440108430564"));
            Assert.IsTrue(aId.Equals("REEF_LOCAL_RUNTIME"));
            Assert.IsTrue(rId.Equals("socket://10.130.68.76:9528"));

            var contextConfigString = evaluatorConfigurations.RootContextConfigurationString;
            var serviceConfigString = evaluatorConfigurations.RootServiceConfigurationString;
            var taskConfigString = evaluatorConfigurations.TaskConfigurationString;

            Assert.IsFalse(string.IsNullOrWhiteSpace(contextConfigString));
            Assert.IsFalse(string.IsNullOrWhiteSpace(taskConfigString));
        }

        [TestMethod, Priority(0), TestCategory("Unit")]
        [DeploymentItem(@"ConfigFiles")]
        public void TestEvaluatorConfigurationFile()
        {
            AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
            var avroConfiguration = serializer.AvroDeseriaizeFromFile("evaluator.conf");

            Assert.IsNotNull(avroConfiguration);
            Assert.AreEqual(avroConfiguration.language, AvroConfigurationSerializer.Java);

            foreach (var b in avroConfiguration.Bindings)
            {
               Logger.Log(Level.Info, "Key = " + b.key + " Value = " + b.value); 
            }
        }
    }
}