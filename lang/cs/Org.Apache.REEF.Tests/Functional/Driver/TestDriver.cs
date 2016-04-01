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
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Defaults;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Driver
{
    [Collection("FunctionalTests")]
    public class TestDriver : ReefFunctionalTest
    {
        /// <summary>
        /// This is to test DriverTestStartHandler. No evaluator and tasks are involved.
        /// </summary>
        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test DriverTestStartHandler. No evaluator and tasks are invoked")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestDriverStart()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurations(), typeof(DriverTestStartHandler), 0, "DriverTestStartHandler", "local", testFolder);
            ValidateSuccessForLocalRuntime(0, testFolder: testFolder);
            CleanUp(testFolder);
        }

        public IConfiguration DriverConfigurations()
        {
            IConfiguration driverConfig = DriverConfiguration.ConfigurationModule
             .Set(DriverConfiguration.OnDriverStarted, GenericType<DriverTestStartHandler>.Class)
             .Set(DriverConfiguration.CustomTraceListeners, GenericType<DefaultCustomTraceListener>.Class)
             .Set(DriverConfiguration.CustomTraceLevel, Level.Info.ToString())
             .Build();
            return driverConfig;
        }
    }
}
