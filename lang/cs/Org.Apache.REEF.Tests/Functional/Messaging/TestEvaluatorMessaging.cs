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

using System.Collections.Generic;
using System.Security.Policy;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Defaults;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Xunit;

namespace Org.Apache.REEF.Tests.Functional.Messaging
{
    [Collection("FunctionalTests")]
    public class TestEvaluatorMessaging : ReefFunctionalTest
    {
        /// <summary>
        /// This test is to test message passing between Evaluator and Driver.
        /// Such messages include Context, Task, and Driver messages. The messages are sent 
        /// from one side and received in the corresponding handlers and verified in the test.
        /// </summary>
        [Fact]
        [Trait("Priority", "1")]
        [Trait("Category", "FunctionalGated")]
        [Trait("Description", "Test messaging between Driver and Evaluators.")]
        //// TODO[JIRA REEF-1184]: add timeout 180 sec
        public void TestSendMessages()
        {
            string testFolder = DefaultRuntimeFolder + TestId;
            TestRun(DriverConfigurations(), typeof(MessageDriver), 1, "sendMessages", "local", testFolder);
            ValidateSuccessForLocalRuntime(1, testFolder: testFolder);

            var expectedDriverLogs = new List<string>
            {
                MessageDriver.SendingMessageToTaskLog,
                MessageDriver.DriverReceivedTaskMessageLog,
                MessageDriver.SendingMessageToContextLog,
                MessageDriver.DriverReceivedContextMessageLog,
            };

            ValidateMessageSuccessfullyLogged(expectedDriverLogs, "driver", DriverStdout, testFolder, -1);

            var expectedEvaluatorLogs = new List<string>
            {
                MessageTask.MessageReceivedFromDriverLog,
                MessageTask.MessageSentToDriverLog,
                MessageContext.MessageReceivedFromDriverLog,
                MessageContext.MessageSentToDriverLog
            };

            ValidateMessageSuccessfullyLogged(expectedEvaluatorLogs, "Node-*", EvaluatorStdout, testFolder, -1);

            CleanUp(testFolder);
        }

        private static IConfiguration DriverConfigurations()
        {
            return DriverConfiguration.ConfigurationModule
            .Set(DriverConfiguration.OnDriverStarted, GenericType<MessageDriver>.Class)
            .Set(DriverConfiguration.OnEvaluatorAllocated, GenericType<MessageDriver>.Class)
            .Set(DriverConfiguration.OnTaskMessage, GenericType<MessageDriver>.Class)
            .Set(DriverConfiguration.OnTaskRunning, GenericType<MessageDriver>.Class)
            .Set(DriverConfiguration.OnContextActive, GenericType<MessageDriver>.Class)
            .Set(DriverConfiguration.OnContextMessage, GenericType<MessageDriver>.Class)
            .Set(DriverConfiguration.CustomTraceListeners, GenericType<DefaultCustomTraceListener>.Class)
            .Set(DriverConfiguration.CustomTraceLevel, Level.Info.ToString())
            .Build();
        }
    }
}