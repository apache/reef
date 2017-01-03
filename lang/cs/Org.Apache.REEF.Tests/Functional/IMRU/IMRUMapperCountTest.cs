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

using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.IMRU.Examples;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    [Collection("FunctionalTests")]
    public class IMRUMapperCountTest : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(IMRUMapperCountTest));

        private static readonly int NumNodes = 4;

        [Fact]
        [Trait("Description", "Run IMRU mapper count example as test.")]
        void TestIMRUMapperCountOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestIMRUMapperCount(false, testFolder);
            ValidateSuccessForLocalRuntime(NumNodes, testFolder: testFolder);
            CleanUp(testFolder);
        }

        [Fact]
        [Trait("Environment", "Yarn")]
        [Trait("Description", "Run IMRU mapper count example as test on Yarn.")]
        void TestIMRUMapperCountOnYarn()
        {
            TestIMRUMapperCount(true);
        }

        private void TestIMRUMapperCount(bool runOnYarn, params string[] testFolder)
        {
            var tcpPortConfig = TcpPortConfigurationModule.ConfigurationModule
                .Set(TcpPortConfigurationModule.PortRangeStart, "8900")
                .Set(TcpPortConfigurationModule.PortRangeCount, "1000")
                .Build();
            Run.RunMapperTest(tcpPortConfig, runOnYarn, NumNodes, " ", testFolder);
        }
    }
}
