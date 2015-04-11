/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using Microsoft.Hadoop.Avro;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.StreamingCodec;
using Org.Apache.REEF.Wake;

namespace Org.Apache.REEF.Network.NetworkService
{
    /// <summary>
    /// Writable Message sent between NetworkServices. This will eventually replace NsMessage<T>
    /// for Group Communication after design is finalized</summary>
    /// <typeparam name="T">The type of data being sent. It is assumed to be Writable</typeparam>
    public class SerializableNsMessage<T> : IWritable where T : IWritable
    {
        /// <summary>
        /// Empty constructor to allow instantiation by reflection
        /// </summary>
        public SerializableNsMessage()
        {   
        }
        
        /// <summary>
        /// Create a new NsMessage with no data.
        /// </summary>
        /// <param name="sourceId">The identifier of the sender</param>
        /// <param name="destId">The identifier of the receiver</param>
        public SerializableNsMessage(IIdentifier sourceId, IIdentifier destId)
        {
            SourceId = sourceId;
            DestId = destId;
            Data = new List<T>();
            MessageCount = 0;
        }

        /// <summary>
        /// Create a new NsMessage with data.
        /// </summary>
        /// <param name="sourceId">The identifier of the sender</param>
        /// <param name="destId">The identifier of the receiver</param>
        /// <param name="message">The message to send</param>
        public SerializableNsMessage(IIdentifier sourceId, IIdentifier destId, T message)
        {
            SourceId = sourceId;
            DestId = destId;
            Data = new List<T> {message};
            MessageCount = 1;
        }

        /// <summary>
        /// The identifier of the sender of the message.
        /// </summary>
        public IIdentifier SourceId { get; private set; }

        /// <summary>
        /// The identifier of the receiver of the message.
        /// </summary>
        public IIdentifier DestId { get; private set; }

        /// <summary>
        /// A list of data being sent in the message.
        /// </summary>
        public List<T> Data { get; set; }

        /// <summary>
        /// The number of messages in the Data list
        /// </summary>
        public int MessageCount;

        /// <summary>
        /// Read the class fields from the stream.
        /// </summary>
        /// <param name="stream">The stream from which to read</param>
        /// <param name="optionalParameters">The optional parameters to be passed to the reader.
        /// There is one optional parameter "IIdentifierFactory" for NsMessage</param>
        public void Read(Stream stream, params object[] optionalParameters)
        {
            if (optionalParameters.Length == 0)
            {
                throw new Exception("Inside Network Service Message Read Function: Expected the optional parameter of type IIdentifierFactory");
            }

            IIdentifierFactory factory = optionalParameters[0] as IIdentifierFactory;

            if (factory == null)
            {
                throw new Exception("Inside Network Service Message Read Function: The optional parameter is not of type IIdentifierFactory");
            }

            SourceId = factory.Create(AuxillaryStreamingFunctions.StreamToString(stream));
            DestId = factory.Create(AuxillaryStreamingFunctions.StreamToString(stream));
            MessageCount = AuxillaryStreamingFunctions.StreamToInt(stream);

            if (Data.Count == 0)
            {
                throw new Exception("Message Count in Network Service Message cannot be zero");
            }

            Data = new List<T>();

            for (int index = 0; index < MessageCount; index++)
            {
                Data.Add(Activator.CreateInstance<T>());

                if (Data[index] == null)
                {
                    throw new Exception("T type instance cannot be created from the stream data in Network Service Message");
                }

                Data[index].Read(stream);
            }
        }

        /// <summary>
        /// Writes the class fields to the stream.
        /// </summary>
        /// <param name="stream">The stream to which to write</param>
        public void Write(Stream stream)
        {
            AuxillaryStreamingFunctions.StringToStream(SourceId.ToString(), stream);
            AuxillaryStreamingFunctions.StringToStream(DestId.ToString(), stream);
            AuxillaryStreamingFunctions.IntToStream(MessageCount, stream);

            foreach (var data in Data)
            {
                data.Write(stream);
            }
        }
    }
}
