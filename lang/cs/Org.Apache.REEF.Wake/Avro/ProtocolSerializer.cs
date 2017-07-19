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
using System.IO;
using System.Reflection;
using Microsoft.Hadoop.Avro;
using Org.Apache.REEF.Utilities.Logging;
using org.apache.reef.wake.avro.message;

namespace Org.Apache.REEF.Wake.Avro
{
    /// <summary>
    /// Given a namespace of Avro messages which represent a protocol, the ProtocolSerailizer
    /// class reflects all of the message classes and builds Avro seriailziers and deserializers.
    /// Avro messages are then serialized/deserilaized to and from byte[] arrays using the
    /// Read/Write methods. A transport such as a RemoteObserver using a ByteCodec can then
    /// be used to send and receive the serialized messages.
    /// </summary>
    public sealed class ProtocolSerializer
    {
        private static readonly Logger Logr = Logger.GetLogger(typeof(ProtocolSerializer));

        // Delagates for message serializers and deserializers.
        private delegate void Serialize(MemoryStream stream, object message);
        private delegate void Deserialize(MemoryStream stream, object observer, long sequence);

        // Message type to serialize/derserialize delagate.
        private readonly SortedDictionary<string, Serialize> serializeMap = new SortedDictionary<string, Serialize>();
        private readonly SortedDictionary<string, Deserialize> deserializeMap = new SortedDictionary<string, Deserialize>();

        private readonly IAvroSerializer<Header> headerSerializer = AvroSerializer.Create<Header>();

        /// <summary>
        /// Register all of the protocol messages using reflection.
        /// </summary>
        /// <param name="assembly">The Assembley object which contains the namespace of the message classes.</param>
        /// <param name="messageNamespace">A string which contains the namespace the protocol messages.</param>
        public ProtocolSerializer(Assembly assembly, string messageNamespace)
        {
            MethodInfo registerInfo = typeof(ProtocolSerializer).GetMethod("Register", BindingFlags.Instance | BindingFlags.NonPublic);
            MethodInfo genericInfo;

            Logr.Log(Level.Info, "Retrieving types for assembly: {0}", assembly.FullName);
            List<Type> types = new List<Type>(assembly.GetTypes());
            types.Add(typeof(Header));

            foreach (Type type in types)
            {
                string name = type.FullName;
                if (name.StartsWith(messageNamespace))
                {
                    genericInfo = registerInfo.MakeGenericMethod(new[] { type });
                    genericInfo.Invoke(this, null);
                }
            }
        }

        /// <summary>
        /// Generate and store the metadata necessary to serialze and deserialize a specific message type.
        /// </summary>
        /// <typeparam name="TMessage">The class type of the message being registered.</typeparam>
        internal void Register<TMessage>()
        {
            Logr.Log(Level.Info, "Registering message type: {0} {1}", typeof(TMessage).FullName, typeof(TMessage).Name);

            IAvroSerializer<TMessage> messageSerializer = AvroSerializer.Create<TMessage>();
            Serialize serialize = (MemoryStream stream, object message) =>
            {
                messageSerializer.Serialize(stream, (TMessage)message);
            };
            serializeMap.Add(typeof(TMessage).Name, serialize);

            Deserialize deserialize = (MemoryStream stream, object observer, long sequence) =>
            {
                TMessage message = messageSerializer.Deserialize(stream);
                IObserver<MessageInstance<TMessage>> msgObserver = observer as IObserver<MessageInstance<TMessage>>;
                if (msgObserver != null)
                {
                    msgObserver.OnNext(new MessageInstance<TMessage>(sequence, message));
                }
                else
                {
                    Logr.Log(Level.Warning, "Unhandled message received: {0}", message);
                }
            };
            deserializeMap.Add(typeof(TMessage).Name, deserialize);
        }

        /// <summary>
        /// Serialize the input message and return a byte array.
        /// </summary>
        /// <param name="message">An object reference to the messeage to be serialized</param>
        /// <param name="sequence">A long which cotains the higher level protocols sequence number for the message.</param>
        /// <returns>A byte array containing the serialized header and message.</returns>
        public byte[] Write(object message, long sequence) 
        {
            string name = message.GetType().Name;
            Logr.Log(Level.Info, "Serializing message: {0}", name);
            try
            { 
                using (MemoryStream stream = new MemoryStream())
                {
                    Header header = new Header(sequence, name);
                    headerSerializer.Serialize(stream, header);

                    Serialize serialize;
                    if (serializeMap.TryGetValue(name, out serialize))
                    {
                        serialize(stream, message);
                    }
                    else
                    {
                        throw new SeializationException("Request to serialize unknown message type: " + name);
                    }
                    return stream.GetBuffer();
                }
            }
            catch (Exception e)
            {
                Logr.Log(Level.Error, "Failure writing message.", e);
                throw e;
            }
        }

        /// <summary>
        /// Read a message from the input byte array.
        /// </summary>
        /// <param name="data">The byte array containing a header message and message to be deserialized.</param>
        /// <param name="observer">An object which implements the IObserver<> interface for the message being deserialized.</param>
        public void Read(byte[] data, object observer)
        {
            try
            {
                using (MemoryStream stream = new MemoryStream(data))
                {
                    Header head = headerSerializer.Deserialize(stream);
                    Deserialize deserialize;
                    if (deserializeMap.TryGetValue(head.className, out deserialize))
                    {
                        deserialize(stream, observer, head.sequence);
                    }
                    else
                    {
                        throw new SeializationException("Request to deserialize unknown message type: " + head.className);
                    }
                }
            }
            catch (Exception e)
            {
                Logr.Log(Level.Error, "Failure reading message.", e);
                throw e;
            }
        }
    }
}
