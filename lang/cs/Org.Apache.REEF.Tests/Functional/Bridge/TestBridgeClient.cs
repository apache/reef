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

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Client.Common;
using Org.Apache.REEF.Examples.AllHandlers;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tests.Functional.Bridge
{
    [TestClass]
    public class TestBridgeClient : ReefFunctionalTest
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(TestBridgeClient));

        [TestInitialize()]
        public void TestSetup()
        {
            Init();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            Console.WriteLine("Post test check and clean up");
        }

        [TestMethod, Priority(1), TestCategory("FunctionalGated")]
        [Description("Run CLR Bridge on local runtime")]
        [DeploymentItem(@".")]
        [Ignore] //This test needs to be run on Yarn environment with test framework installed.
        public void CanRunClrBridgeExampleOnYarn()
        {
            string testRuntimeFolder = DefaultRuntimeFolder + TestNumber++;
            RunClrBridgeClient(true, testRuntimeFolder);
        }

        [TestMethod, Priority(1), TestCategory("FunctionalGated")]
        [Description("Run CLR Bridge on local runtime")]
        [DeploymentItem(@".")]
        [Timeout(180 * 1000)]
        public void CanRunClrBridgeExampleOnLocalRuntime()
        {
            string testRuntimeFolder = DefaultRuntimeFolder + TestNumber++;
            CleanUp(testRuntimeFolder);
            RunClrBridgeClient(false, testRuntimeFolder);
        }

        private async void RunClrBridgeClient(bool runOnYarn, string testRuntimeFolder)
        {
            string[] a = new[] { runOnYarn ? "yarn" : "local", testRuntimeFolder };
            IJobSubmissionResult driverHttpEndpoint = AllHandlers.Run(a);

            var uri = driverHttpEndpoint.DriverUrl + "NRT/status?a=1&b=2";
            var strStatus = driverHttpEndpoint.GetUrlResult(uri);
            Assert.IsTrue(strStatus.Equals("Byte array returned from HelloHttpHandler in CLR!!!\r\n"));

            await ((JobSubmissionResult)driverHttpEndpoint).TryUntilNoConnection(uri);

            ValidateSuccessForLocalRuntime(0, testFolder: testRuntimeFolder);

            CleanUp(testRuntimeFolder);
        }
    }
}
