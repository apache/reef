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
using System;
using Org.Apache.REEF.Bridge.Core.Common.Client.Config.Runtime;
using Org.Apache.REEF.Bridge.Core.Tests.Fail.Driver;
using Org.Apache.REEF.Bridge.Core.Tests.Fail.ThreadInterruptedException;
using Org.Apache.REEF.Common.Client;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Wake.Time.Event;
using Xunit;

namespace Org.Apache.REEF.Bridge.Core.Tests.Fail.Test
{
    public sealed class FailDriverTest
    {
        private static readonly TimeSpan JobTimeout = TimeSpan.FromMinutes(5);

        private static void FailOn(Type type)
        {
            var launchStatus = FailDriverClient.RunClient(type, LocalRuntimeConfiguration.ConfigurationModule.Build(), JobTimeout);
            TestUtils.AssertLauncherFailure(launchStatus, typeof(SimulatedDriverFailure));
        }

        [Fact]
        public void TestFailDriverConstructor()  {
            FailOn(typeof(FailDriver));
        }

        [Fact]
        public void TestFailDriverStart()  {
            FailOn(typeof(IDriverStarted));
        }

        [Fact]
        public void TestFailDriverAllocatedEvaluator()  {
            FailOn(typeof(IAllocatedEvaluator));
        }

        [Fact]
        public void TestFailDriverActiveContext()  {
            FailOn(typeof(IActiveContext));
        }

        [Fact]
        public void TestFailDriverRunningTask()  {
            FailOn(typeof(IRunningTask));
        }

        [Fact]
        public void TestFailDriverTaskMessage()  {
            FailOn(typeof(ITaskMessage));
        }

        [Fact]
        public void TestFailDriverSuspendedTask()  {
            FailOn(typeof(ISuspendedTask));
        }

        [Fact]
        public void TestFailDriverCompletedTask()  {
            FailOn(typeof(ICompletedTask));
        }

        [Fact]
        public void TestFailDriverCompletedEvaluator()  {
            FailOn(typeof(ICompletedEvaluator));
        }

        [Fact]
        public void TestFailDriverAlarm()  {
            FailOn(typeof(Alarm));
        }

        [Fact]
        public void TestFailDriverStop() {
            FailOn(typeof(IDriverStopped));
        }

        [Fact]
        public void TestDriverCompleted()  {
            // NoopTask can be replaced with any other class never used in FailDriver
            LauncherStatus status = FailDriverClient.RunClient(typeof(NoopTask), 
                LocalRuntimeConfiguration.ConfigurationModule.Build(), JobTimeout);
            Assert.Equal(LauncherStatus.CompletedStatus, status);
        }
    }
}
