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

using Org.Apache.Reef.Driver;
using Org.Apache.Reef.Utilities.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Org.Apache.Reef.Test
{
    [TestClass]
    public class TestBridgeClient : ReefFunctionalTest
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(TestBridgeClient));

        [TestInitialize()]
        public void TestSetup()
        {
            CleanUp();
            Init();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            Console.WriteLine("Post test check and clean up");
            CleanUp();
        }

        [TestMethod, Priority(1), TestCategory("FunctionalGated")]
        [Description("Run CLR Bridge on local runtime")]
        [DeploymentItem(@".")]
        [Ignore] // This is diabled by default on builds
        public void CanRunClrBridgeOnYarn()
        {
            RunClrBridgeClient(runOnYarn: true);
        }

        [TestMethod, Priority(1), TestCategory("FunctionalGated")]
        [Description("Run CLR Bridge on local runtime")]
        [DeploymentItem(@".")]
        [Timeout(180 * 1000)]
        public void CanRunClrBridgeOnLocalRuntime()
        {
            IsOnLocalRuntiime = true;
            RunClrBridgeClient(runOnYarn: false);
            ValidateSuccessForLocalRuntime(2);
        }

        private void RunClrBridgeClient(bool runOnYarn)
        {
            const string clrBridgeClient = "Org.Apache.Reef.CLRBridgeClient.exe";
            List<string> arguments = new List<string>();
            arguments.Add(runOnYarn.ToString());
            arguments.Add(Constants.BridgeLaunchClass);
            arguments.Add(".");
            arguments.Add(Path.Combine(_binFolder, Constants.BridgeJarFileName));
            arguments.Add(Path.Combine(_binFolder, _cmdFile));

            ProcessStartInfo startInfo = new ProcessStartInfo()
            {
                FileName = clrBridgeClient,
                Arguments = string.Join(" ", arguments),
                RedirectStandardOutput = true,
                UseShellExecute = false,
                CreateNoWindow = false
            }; 
            
            LOGGER.Log(Level.Info, "executing\r\n" + startInfo.FileName + "\r\n" + startInfo.Arguments);
            using (Process process = Process.Start(startInfo))
            {
                process.WaitForExit();
                if (process.ExitCode != 0)
                {
                    throw new InvalidOperationException("CLR client exited with error code " + process.ExitCode);
                }
            }
        }
    }
}
