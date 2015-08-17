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
using System.Globalization;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.REEF.IMRU.Examples.MapperCount;
using Org.Apache.REEF.IMRU.OnREEF.Client;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Parameters;

namespace Org.Apache.REEF.Tests.Functional.IMRU
{
    [TestClass]
    public class MapperCountTest : ReefFunctionalTest
    {
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
        [Description("Run Mapper Count on yarn runtime")]
        [DeploymentItem(@".")]
        [Ignore] //This test needs to be run on Yarn environment with test framework installed.
        public void CanRunIMRUClientOnYarn()
        {
            int numNodes = 5;
            int startPort = 8900;
            int portRange = 1000;

            var config = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter(typeof (TcpPortRangeStart),
                    startPort.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter(typeof (TcpPortRangeCount),
                    portRange.ToString(CultureInfo.InvariantCulture))
                .Build();

            RunIMRUClient(true, numNodes, config);
        }

        [TestMethod, Priority(1), TestCategory("FunctionalGated")]
        [Description("Run Mapper Count on local runtime")]
        [DeploymentItem(@".")]
        [Timeout(180*1000)]
        public void CanRunIMRUClientOnLocalRuntime()
        {
            int numNodes = 2;
            IConfiguration config = TangFactory.GetTang().NewConfigurationBuilder().Build();
            RunIMRUClient(false, numNodes, config);
        }

        private void RunIMRUClient(bool runOnYarn, int numNodes, IConfiguration externalConfig)
        {
            string testRuntimeFolder = DefaultRuntimeFolder + TestNumber++;
            MapperCount tested = null;

            if (!runOnYarn)
            {
                tested =
                    TangFactory.GetTang()
                        .NewInjector(new[]
                        {
                            OnREEFIMRURunTimeConfigurations<int, int, int>.GetLocalIMRUConfiguration(numNodes,
                                testRuntimeFolder),
                            externalConfig
                        }).GetInstance<MapperCount>();
            }
            else
            {
                tested = TangFactory.GetTang()
                    .NewInjector(new[]
                    {
                        OnREEFIMRURunTimeConfigurations<int, int, int>.GetYarnIMRUConfiguration(),
                        externalConfig
                    }).
                    GetInstance<MapperCount>();
            }

            tested.Run(numNodes - 1);

            ValidateSuccessForLocalRuntime(numNodes, testRuntimeFolder);
            CleanUp(testRuntimeFolder);
        }
    }
}
