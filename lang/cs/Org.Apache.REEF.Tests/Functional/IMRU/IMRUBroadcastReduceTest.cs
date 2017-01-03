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

using System.Linq;
using Org.Apache.REEF.Client.API;
using Org.Apache.REEF.IMRU.Examples;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    [Collection("FunctionalTests")]
    public class IMRUBroadcastReduceTest : ReefFunctionalTest
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(IMRUBroadcastReduceTest));

        private static readonly int NumNodes = 10;
        private static readonly int NumOfRetry = 2;

        [Fact]
        [Trait("Description", "Run IMRU broadcast and reduce example as test.")]
        void TestIMRUBroadcastReduceOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestIMRUBroadcastReduce(false, false, true, testFolder);
            ValidateSuccessForLocalRuntime(NumNodes, testFolder: testFolder);
            CleanUp(testFolder);
        }

        [Fact]
        [Trait("Description", "Run IMRU broadcast and reduce example as test.")]
        void TestIMRUBroadcastReduceWithFTOnLocalRuntime()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestIMRUBroadcastReduce(false, true, true, testFolder);
            string[] lines = ReadLogFile(DriverStdout, "driver", testFolder, 240);
            var completedTaskCount = GetMessageCount(lines, "Received ICompletedTask");
            var runningTaskCount = GetMessageCount(lines, "Received IRunningTask");
            var failedEvaluatorCount = GetMessageCount(lines, "Received IFailedEvaluator");
            var failedTaskCount = GetMessageCount(lines, "Received IFailedTask");
            Assert.True((NumOfRetry + 1) * NumNodes >= completedTaskCount + failedEvaluatorCount + failedTaskCount);
            Assert.True(NumOfRetry * NumNodes < completedTaskCount + failedEvaluatorCount + failedTaskCount);
            Assert.Equal((NumOfRetry + 1) * NumNodes, runningTaskCount);
            CleanUp(testFolder);
        }

        [Fact]
        [Trait("Description", "Run IMRU broadcast and reduce example with random failures as test.")]
        void TestImruBroadcastReduceWithRandFailuresOnLocalRuntime()
        {
            var testFolder = DefaultRuntimeFolder + TestId;
            TestIMRUBroadcastReduce(false, true, false, testFolder);
            var lines = ReadLogFile(DriverStdout, "driver", testFolder, 240);
            var completedEvaluatorCount = GetMessageCount(lines, "Received CompletedEvaluator");
            Assert.Equal(NumNodes, completedEvaluatorCount);
            CleanUp(testFolder);
        }

        [Fact(Skip = "Requires Yarn")]
        [Trait("Description", "Run IMRU broadcast and reduce example as test on Yarn.")]
        void TestIMRUBroadcastReduceOnYarn()
        {
            TestIMRUBroadcastReduce(true, false, true);
        }

        private void TestIMRUBroadcastReduce(bool runOnYarn, bool faultTolerant, bool fixedFailures, params string[] testFolder)
        {
            var tcpPortConfig = TcpPortConfigurationModule.ConfigurationModule
                .Set(TcpPortConfigurationModule.PortRangeStart, "8900")
                .Set(TcpPortConfigurationModule.PortRangeCount, "1000")
                .Build();

            string[] commonArgs = { "10", "2", "512", "512", "100" };
            var args = fixedFailures
                ? commonArgs.Concat(new string[] { NumOfRetry.ToString(), NumOfRetry.ToString() }).ToArray()
                : commonArgs.Concat(new string[] { "0.6", "10" }).ToArray();
            Run.RunBroadcastReduceTest(tcpPortConfig, runOnYarn, NumNodes, faultTolerant, fixedFailures, args, testFolder);
        }
    }
}
