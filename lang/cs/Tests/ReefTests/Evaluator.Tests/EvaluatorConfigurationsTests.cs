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

using Org.Apache.Reef.Common;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Org.Apache.Reef.Test
{
    [TestClass]
    public class EvaluatorConfigurationsTests
    {
        [TestMethod, Priority(0), TestCategory("Unit")]
        [DeploymentItem(@"ConfigFiles")]
        public void TestEvaluatorConfigurations()
        {
            EvaluatorConfigurations evaluatorConfigurations = new EvaluatorConfigurations("evaluator.conf");

            Assert.IsTrue(evaluatorConfigurations.EvaluatorId.Equals("Node-1-1414443998204"));

            Assert.IsTrue(evaluatorConfigurations.ApplicationId.Equals("REEF_LOCAL_RUNTIME"));

            string rootContextConfigString = evaluatorConfigurations.RootContextConfiguration;
            Assert.IsFalse(string.IsNullOrWhiteSpace(rootContextConfigString));
        }
    }
}