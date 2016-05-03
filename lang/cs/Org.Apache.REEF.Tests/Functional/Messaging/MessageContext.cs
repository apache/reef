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
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tests.Functional.Messaging
{
    /// <summary>
    /// A Context event handler class that generates context messages for the Driver 
    /// and receives context messages from the Driver for testing.
    /// </summary>
    public sealed class MessageContext : IContextMessageHandler, IContextMessageSource
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MessageContext));

        public const string MessageSend = "MESSAGE:CONTEXT";

        public const string MessageSentToDriverLog = "Message sent to Driver from Context.";
        public const string MessageReceivedFromDriverLog = "Message received from Driver in Context.";

        private readonly MessageManager _messageManager;

        [Inject]
        private MessageContext(MessageManager messageManager)
        {
            _messageManager = messageManager;
        }

        public Optional<ContextMessage> Message
        {
            get
            {
                Logger.Log(Level.Info, MessageSentToDriverLog);
                _messageManager.OnContextMessageSent();

                return Optional<ContextMessage>.Of(
                    ContextMessage.From("Context", ByteUtilities.StringToByteArrays(MessageSend)));
            }
        }

        public void OnNext(byte[] messageBytes)
        {
            var message = ByteUtilities.ByteArraysToString(messageBytes);
            Logger.Log(Level.Info, MessageReceivedFromDriverLog);

            if (!message.Equals(MessageDriver.Message))
            {
                Exceptions.Throw(
                    new Exception("Unexpected driver message: " + message), 
                    "Unexpected driver message received: " + message,
                    Logger);
            }
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }
    }
}