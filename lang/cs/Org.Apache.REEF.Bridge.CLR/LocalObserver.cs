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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Avro;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Bridge
{
    /// <summary>
    /// The Local Observer class receives byte buffer messages from the transport layer,
    /// deserializes the messages into Avro C# classes, and invokes the appropriate
    /// IObserver callback on the Avro message observer.
    /// </summary>
    public sealed class LocalObserver : IObserver<IRemoteMessage<byte[]>>
    {
        /// <summary>
        /// Specify the class that will be called to process deserialied Avro messages.
        /// </summary>
        [NamedParameter(Documentation =
            "Must implement IObserver<IMessageInstance<TMessage>> for messages to be received.")]
        public class MessageObserver : Name<object>
        {
        }

        private static readonly Logger Logger = Logger.GetLogger(typeof(LocalObserver));

        private readonly ProtocolSerializer _serializer;
        private readonly IInjectionFuture<object> _fMessageObserver;

        /// <summary>
        /// Injection constructor.
        /// </summary>
        /// <param name="serializer">The protocol serializer instance to be used.</param>
        /// <param name="fMessageObserver">An injection future with message observer to be
        /// called to process Avro messages from the Java bridge.</param>
        [Inject]
        public LocalObserver(
            ProtocolSerializer serializer,
            [Parameter(typeof(MessageObserver))] IInjectionFuture<object> fMessageObserver)
        {
            _serializer = serializer;
            _fMessageObserver = fMessageObserver;
        }

        /// <summary>
        /// Called by the remote manager to process messages received from the java bridge.
        /// </summary>
        /// <param name="message">A byte buffer containing a serialied message.</param>
        public void OnNext(IRemoteMessage<byte[]> message)
        {
            Logger.Log(Level.Verbose, "Message received: {0}", message.Identifier);

            // Deserialize the message and invoke the appropriate handler.
            _serializer.Read(message.Message, _fMessageObserver.Get());
        }

        /// <summary>
        /// Handles error conditions in the low transport layer.
        /// </summary>
        /// <param name="error">The exception generated in the transport layer.</param>
        public void OnError(Exception error)
        {
            Logger.Log(Level.Error, "Error: ", error);
        }

        /// <summary>
        /// Notification that no nore message prpocessing is required.
        /// </summary>
        public void OnCompleted()
        {
            Logger.Log(Level.Info, "Completed");
        }
    }
}