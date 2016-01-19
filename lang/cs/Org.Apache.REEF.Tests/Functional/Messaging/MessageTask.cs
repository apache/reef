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
using System.Globalization;
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Examples.Tasks.HelloTask;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tests.Functional.Messaging
{
    public class MessageTask : ITask, ITaskMessageSource
    {
        public const string MessageSend = "MESSAGE:TASK";

        private static readonly Logger LOGGER = Logger.GetLogger(typeof(MessageTask));

        [Inject]
        public MessageTask()
        {
        }

        public HelloService Service { get; set; }

        public Optional<TaskMessage> Message
        {
            get
            {
                TaskMessage defaultTaskMessage = TaskMessage.From(
                    "messagingSourceId",
                    ByteUtilities.StringToByteArrays(MessageSend + " generated at " + DateTime.Now.ToString(CultureInfo.InvariantCulture)));
                return Optional<TaskMessage>.Of(defaultTaskMessage);
            }

            set
            {
            }
        }

        public byte[] Call(byte[] memento)
        {
            Console.WriteLine("Hello, CLR TaskMsg!");
            Thread.Sleep(5 * 1000);
            return null;
        }

        public void Dispose()
        {
            LOGGER.Log(Level.Info, "TaskMsg disposed.");
        }

        private void DriverMessage(string message)
        {
            LOGGER.Log(Level.Info, "Received DriverMessage in TaskMsg: " + message);
            if (!message.Equals(MessageDriver.Message))
            {
                Exceptions.Throw(new Exception("Unexpected driver message: " + message), "Unexpected driver message received: " + message, LOGGER);
            }
        }

        public class MessagingDriverMessageHandler : IDriverMessageHandler
        {
            private readonly MessageTask _parentTask;

            [Inject]
            public MessagingDriverMessageHandler(MessageTask task)
            {
                _parentTask = task;
            }

            public void Handle(IDriverMessage value)
            {
                string message = string.Empty;
                LOGGER.Log(Level.Verbose, "Received a message from driver, handling it with MessagingDriverMessageHandler");
                if (value.Message.IsPresent())
                {
                    message = ByteUtilities.ByteArraysToString(value.Message.Value);
                }
                _parentTask.DriverMessage(message);
            }
        }
    }
}