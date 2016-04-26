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
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Defaults;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Messaging
{
    [Collection("FunctionalTests")]
    public class TestTaskMessage : ReefFunctionalTest
    {
        /// <summary>
        /// This test is to test both task message and driver message. The messages are sent 
        /// from one side and received in the corresponding handlers and verified in the test 
        /// </summary>
        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test task message and driver message")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestSendTaskMessage()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurations(), typeof(MessageDriver), 1, "simpleHandler", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, testFolder: testFolder);

            var messages = new List<string>();
            messages.Add("TaskMessagingTaskMessageHandler received following message from Task:");
            messages.Add("Message: MESSAGE:TASK generated");
            messages.Add("is to send message MESSAGE::DRIVER");      
            ValidateMessageSuccessfullyLogged(messages, "driver", DriverStdout, testFolder, -1);

            var messages2 = new List<string>();
            messages.Add("Received a message from driver, handling it with MessagingDriverMessageHandler:MESSAGE::DRIVER");
            messages.Add("Message is sent back from task to driver:");
            ValidateMessageSuccessfullyLogged(messages2, "Node-*", EvaluatorStdout, testFolder, -1);

            CleanUp(testFolder);
        }

        private IConfiguration DriverConfigurations()
        {
            return DriverConfiguration.ConfigurationModule
            .Set(DriverConfiguration.OnDriverStarted, GenericType<MessageDriver>.Class)
            .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<MessageDriver>.Class)
            .Set(DriverConfiguration.OnTaskMessage, GenericType<MessageDriver>.Class)
            .Set(DriverConfiguration.OnTaskRunning, GenericType<MessageDriver>.Class)
            .Set(DriverConfiguration.CustomTraceListeners, GenericType<DefaultCustomTraceListener>.Class)
            .Set(DriverConfiguration.CustomTraceLevel, Level.Info.ToString())
            .Build();
        }
    }
}