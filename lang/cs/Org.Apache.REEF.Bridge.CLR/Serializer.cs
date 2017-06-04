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
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Bridge;
using org.apache.reef.bridge.message;
using Microsoft.Hadoop.Avro;

namespace Org.Apache.REEF.Bridge
{
    /// <summary>
    /// Serializes and desearializes distributed R messages with a header
    /// that identifies the message type. All messages types are automatically
    /// registered via type reflection at startup.
    /// </summary>
    public static class Serializer
    {
        private static readonly Logger Logr = Logger.GetLogger(typeof(Serializer));

        // Delagates for message serializers and deserializers.
        private delegate void Serialize(MemoryStream stream, object message);
        private delegate void Deserialize(MemoryStream stream, object observer, long identifier);

        // Message type to serialize/derserialize delagate.
        private static SortedDictionary<string, Serialize> serializeMap = new SortedDictionary<string, Serialize>();
        private static SortedDictionary<string, Deserialize> deserializeMap = new SortedDictionary<string, Deserialize>();

        /// <summary>
        /// Register all of the messages in org.apache.reef.bridge.message using reflection.
        /// </summary>
        public static void Initialize()
        {
            MethodInfo registerInfo = typeof(Serializer).GetMethod("Register", BindingFlags.Static | BindingFlags.NonPublic);
            MethodInfo genericInfo;

            Assembly assembly = typeof(Serializer).Assembly;
            Logr.Log(Level.Info, string.Format("Retrieving types for assembly: {0}", assembly.FullName));
            Type[] types = assembly.GetTypes();
            foreach (Type type in types)
            {
                string name = type.FullName;
                if (name.Contains("org.apache.reef.bridge.message"))
                {
                    genericInfo = registerInfo.MakeGenericMethod(new[] { type });
                    if (name.Contains("Protocol"))
                    {
                        // Protocol messages always have index 100.
                        genericInfo.Invoke(null, null);
                    }
                    else
                    {
                        genericInfo.Invoke(null, null);
                    }
                }
            }
        }

        /// <summary>
        /// Generate and store the metadata necessary to serialze and deserialize a specific message type.
        /// </summary>
        /// <typeparam name="TMessage">The class type of the message being registered.</typeparam>
        internal static void Register<TMessage>()
        {
            Logr.Log(Level.Info, string.Format("Registering message type: {0} {1}", typeof(TMessage).FullName, typeof(TMessage).Name));

            Serialize serialize = (MemoryStream stream, object message) =>
            {
                IAvroSerializer<TMessage> messageWriter = AvroSerializer.Create<TMessage>();
                messageWriter.Serialize(stream, (TMessage)message);
            };
            serializeMap.Add(typeof(TMessage).Name, serialize);

            Deserialize deserialize = (MemoryStream stream, object observer, long identifier) =>
            {
                IAvroSerializer<TMessage> messageReader = AvroSerializer.Create<TMessage>();
                TMessage message = messageReader.Deserialize(stream);
                IObserver<Message<TMessage>> msgObserver = observer as IObserver<Message<TMessage>>;
                if (msgObserver != null)
                {
                    msgObserver.OnNext(new Message<TMessage>(identifier, message));
                }
                else
                {
                    Logr.Log(Level.Info, string.Format("Unhandled message received: " + message.ToString()));
                }
            };
            deserializeMap.Add(typeof(TMessage).Name, deserialize);
        }

        /// <summary>
        /// Serialize the input message and return a byte array.
        /// </summary>
        /// <param name="message">A object reference to a messeage to be serialized</param>
        /// <returns>A byte array containing the serialized header and message.</returns>
        public static byte[] Write(object message, long identifier) 
        {
            Logr.Log(Level.Info, "Serializing message: " + message.GetType().Name);
            try
            { 
                IAvroSerializer<Header> headWriter = AvroSerializer.Create<Header>();
                using (MemoryStream stream = new MemoryStream())
                {
                    Header header = new Header(identifier, message.GetType().Name);
                    headWriter.Serialize(stream, header);

                    Serialize serialize;
                    if (serializeMap.TryGetValue(message.GetType().Name, out serialize))
                    {
                        serialize(stream, message);
                    }
                    else
                    {
                        Logr.Log(Level.Info, "Request to seriaiize message of unknown type: " + message.GetType().Name);
                        throw new Exception("Request to serialize unknown message type.");
                    }
                    return stream.GetBuffer();
                }
            }
            catch (Exception e)
            {
                Logr.Log(Level.Error, "Failure writing message: " + e.Message);
                throw e;
            }
        }

        /// <summary>
        /// Read a message from the input byte array.
        /// </summary>
        /// <param name="data">The byte array containing a header message and message to be deserialized.</param>
        /// <param name="observer">An object which implements the IObserver<> interface for the message being deserialized.</param>
        public static void Read(byte[] data, object observer)
        {
            try
            {
                IAvroSerializer<Header> headReader = AvroSerializer.Create<Header>();
                using (MemoryStream stream = new MemoryStream(data))
                {
                    Header head = headReader.Deserialize(stream);
                    Deserialize deserialize;
                    if (deserializeMap.TryGetValue(head.className, out deserialize))
                    {
                        deserialize(stream, observer, head.identifier);
                    }
                    else
                    {
                        throw new Exception("Request to deserialize unknown message type.");
                    }
                }
            }
            catch (Exception e)
            {
                Logr.Log(Level.Error, "Failure reading message: " + e.Message);
                throw e;
            }
        }
    }
}
