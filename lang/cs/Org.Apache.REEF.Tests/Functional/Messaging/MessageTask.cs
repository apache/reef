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

        // TODO[JIRA REEF-1385]: Check the MessageTaskSourceID on the Driver side.
        public const string MessageTaskSourceId = "MessageTaskSourceId";

        public const string MessageSentToDriverLog = "Message sent to Driver from Task.";
        public const string MessageReceivedFromDriverLog = "Message received from Driver in Task.";

        private readonly ManualResetEventSlim _messageFromDriverEvent = new ManualResetEventSlim(false);
        private readonly TestMessageEventManager _messageManager;

        [Inject]
        private MessageTask(TestMessageEventManager messageManager)
        {
            _messageManager = messageManager;
        }

        public Optional<TaskMessage> Message
        {
            get
            {
                var defaultTaskMessage = TaskMessage.From(
                    MessageTaskSourceId,
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
            WaitHandle.WaitAll(new[]
            {
                _messageManager.IsContextMessageSentEvent,
                _messageManager.IsTaskMessageSentEvent,
                _messageFromDriverEvent.WaitHandle
            });

            return null;
        }

        public void Handle(IDriverMessage value)
        {
            try
            {
                if (!value.Message.IsPresent())
                {
                    throw new Exception("Expecting message from Driver but got missing message.");
                }

                var message = ByteUtilities.ByteArraysToString(value.Message.Value);
                if (!message.Equals(MessageDriver.Message))
                {
                    Exceptions.Throw(new Exception("Unexpected driver message: " + message),
                        "Unexpected driver message received: " + message,
                        Logger);
                }
                else
                {
                    Logger.Log(Level.Info, MessageReceivedFromDriverLog);
                }
            }
            finally
            {
                _messageFromDriverEvent.Set();
            }
        }

        public void Dispose()
        {
        }
    }
}