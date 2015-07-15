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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.Examples.HelloCLRBridge;
using Org.Apache.REEF.Tang.Util;
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
        [Ignore] //This test needs to be run on Yarn environment with test framework installed.
        public void CanRunClrBridgeExampleOnYarn()
        {
            RunClrBridgeClient(true);
            ValidateSuccessForLocalRuntime(2);
        }

        [TestMethod, Priority(1), TestCategory("FunctionalGated")]
        [Description("Run CLR Bridge on local runtime")]
        [DeploymentItem(@".")]
        [Timeout(180 * 1000)]
        public void CanRunClrBridgeExampleOnLocalRuntime()
        {
            RunClrBridgeClient(false);
            ValidateSuccessForLocalRuntime(2);
        }

        private void RunClrBridgeClient(bool runOnYarn)
        {
            string[] a = new[] { runOnYarn ? "yarn" : "local" };
            ClrBridgeClient.Run(a);
        }

        /// <summary>
        /// This is to test all the assemblies in the test bin folder can be loaded when creating class hierarchy
        /// </summary>
        [TestMethod, Priority(1), TestCategory("FunctionalGated")]
        [DeploymentItem(@".")]
        [Timeout(180 * 1000)]
        public void TestLoadAssembliesFromTestFolder()
        {
            var files = new HashSet<string>(Directory.GetFiles(Directory.GetCurrentDirectory())
                .Where(e => !(string.IsNullOrWhiteSpace(e)))
                .Select(Path.GetFullPath)
                .Where(File.Exists)
                .Where(IsAssembly)
                .Select(Path.GetFileNameWithoutExtension));

            var loader = new AssemblyLoader(files.ToArray());
            List<Type> types = new List<Type>();
            foreach (var g in loader.Assemblies)
            {
                var ts = g.GetTypes();
                foreach (var t in ts)
                {
                    types.Add(t);
                }
            }
        }

        private static Boolean IsAssembly(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
            {
                return false;
            }
            var extension = Path.GetExtension(path).ToLower();
            return extension.EndsWith("dll") || extension.EndsWith("exe");
        }
    }
}
