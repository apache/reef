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

using System.Threading.Tasks;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Examples.AllHandlers;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Bridge
{
    [Collection("FunctionalTests")]
    public class TestBridgeClient : ReefFunctionalTest
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(TestBridgeClient));

        [Fact(Skip = "Requires Yarn")]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Run CLR Bridge on Yarn")]
        public async Task CanRunClrBridgeExampleOnYarn()
        {
            string testRuntimeFolder = DefaultRuntimeFolder + TestId;
            await RunClrBridgeClient(true, testRuntimeFolder);
        }

        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Run CLR Bridge on local runtime")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public async Task CanRunClrBridgeExampleOnLocalRuntime()
        {
            string testRuntimeFolder = DefaultRuntimeFolder + TestId;
            await RunClrBridgeClient(false, testRuntimeFolder);
        }

        private async Task RunClrBridgeClient(bool runOnYarn, string testRuntimeFolder)
        {
            string[] a = { runOnYarn ? "yarn" : "local", testRuntimeFolder };
            IJobSubmissionResult driverHttpEndpoint = AllHandlers.Run(a);

            var uri = driverHttpEndpoint.DriverUrl + "NRT/status?a=1&b=2";
            var strStatus = driverHttpEndpoint.GetUrlResult(uri);
            Assert.NotNull(strStatus);
            Assert.True(strStatus.Equals("Byte array returned from HelloHttpHandler in CLR!!!\r\n"));

            await((JobSubmissionResult)driverHttpEndpoint).TryUntilNoConnection(uri);

            ValidateSuccessForLocalRuntime(2, testFolder: testRuntimeFolder);

            CleanUp(testRuntimeFolder);
        }
    }
}
