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
using Org.Apache.REEF.Bridge.Core.Common.Client.Config.Runtime;
using Org.Apache.REEF.Bridge.Core.Tests.Fail.Task;
using Org.Apache.REEF.Bridge.Core.Tests.Fail.ThreadInterruptedException;
using System;
using Xunit;

namespace Org.Apache.REEF.Bridge.Core.Tests.Fail.Test
{
    public sealed class FailTaskTest
    {
        private static readonly TimeSpan JobTimeout = TimeSpan.FromMinutes(5);

        private static void FailOn(Type type)
        {
            var launchStatus = FailTaskClient.Run(type, LocalRuntimeConfiguration.ConfigurationModule.Build(), JobTimeout);
            TestUtils.AssertLauncherFailure(launchStatus, typeof(SimulatedTaskFailure));
        }

        [Fact]
        public void TestFailTask()
        {
            FailOn(typeof(FailTask));
        }

        [Fact]
        public void TestFailTaskCall()
        {
            FailOn(typeof(FailTaskCall));
        }

        [Fact]
        public void TestFailTaskMsg()
        {
            FailOn(typeof(FailTaskMsg));
        }

        [Fact]
        public void TestFailTaskSuspend()
        {
            FailOn(typeof(FailTaskSuspend));
        }

        [Fact]
        public void TestFailTaskStart()
        {
            FailOn(typeof(FailTaskStart));
        }

        [Fact]
        public void TestFailTaskStop()
        {
            FailOn(typeof(FailTaskStop));
        }

        [Fact]
        public void TestFailTaskClose()
        {
            FailOn(typeof(FailTaskClose));
        }
    }
}