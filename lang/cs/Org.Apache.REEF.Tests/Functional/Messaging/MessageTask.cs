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
    /// <summary>
    /// A Task that sends messages to the Driver and receives messages from the
    /// Driver for testing.
    /// </summary>
    public class MessageTask : ITask, ITaskMessageSource, IDriverMessageHandler
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MessageTask));

        public const string MessageSend = "MESSAGE:TASK";

        public const string MessageSentToDriverLog = "Message sent to Driver from Task.";
        public const string MessageReceivedFromDriverLog = "Message received from Driver in Task.";

        private readonly MessageManager _messageManager;

        [Inject]
        private MessageTask(MessageManager messageManager)
        {
            _messageManager = messageManager;
        }

        public Optional<TaskMessage> Message
        {
            get
            {
                var defaultTaskMessage = TaskMessage.From(
                    "messagingSourceId",
                    ByteUtilities.StringToByteArrays(MessageSend));
                    
                Logger.Log(Level.Info, MessageSentToDriverLog);
                _messageManager.OnTaskMessageSent();

                return Optional<TaskMessage>.Of(defaultTaskMessage);
            }

            set
            {
            }
        }

        public byte[] Call(byte[] memento)
        {
            while (!(_messageManager.IsTaskMessageSent && _messageManager.IsContextMessageSent))
            {
                // Do not exit until task and context messages are both sent.
                Thread.Sleep(5000);
            }

            return null;
        }

        public void Handle(IDriverMessage value)
        {
            if (!value.Message.IsPresent())
            {
                throw new Exception("Expecting message from Driver but got missing message.");
            }

            Logger.Log(Level.Info, MessageReceivedFromDriverLog);

            var message = ByteUtilities.ByteArraysToString(value.Message.Value);
            if (!message.Equals(MessageDriver.Message))
            {
                Exceptions.Throw(new Exception("Unexpected driver message: " + message), "Unexpected driver message received: " + message, Logger);
            }
        }

        public void Dispose()
        {
        }
    }
}