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
using System.Linq;
using Microsoft.Hadoop.Avro;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.StreamingCodec;
using Org.Apache.REEF.Network.StreamingCodec.Impl;
using Org.Apache.REEF.Wake;
using Org.Apache.REEF.Wake.Remote;
using ProtoBuf;

namespace Org.Apache.REEF.Network.NetworkService.Codec
{
    /// <summary>
    /// Streaming Codec to serialize NsMessages for NetworkService.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public class StreamingNsMessageCodec<T> : StreamingDataCodec<StreamingNsMessage<T>>
    {
        private readonly StreamingDataCodec<T> _codec;
        private readonly IIdentifierFactory _idFactory;

        /// <summary>
        /// Create new NsMessageCodec.
        /// </summary>
        /// <param name="codec">The streaming codec used to serialize message data</param>
        /// <param name="idFactory">Used to create identifier from string.</param>
        public StreamingNsMessageCodec(StreamingDataCodec<T> codec, IIdentifierFactory idFactory)
        {
            _codec = codec;
            _idFactory = idFactory;
        }

        /// <summary>
        /// Serialize the NsMessage.
        /// </summary>
        /// <param name="obj">The object to serialize</param>
        /// <param name="stream">The stream from which to read the data</param>
        /// <returns>The serialized object in byte array form</returns>
        public override void Encode(StreamingNsMessage<T> obj, StreamWithPosition stream)
        {
            obj.MessageCount = obj.Data.Count;
            var avroSerializer = AvroSerializer.Create<StreamingNsMessage<T>>();
            avroSerializer.Serialize(stream.CodecStream, obj);

            foreach (var message in obj.Data)
            {
                _codec.Encode(message, stream);
            }
        }

        /// <summary>
        /// De-serialize the NsMessage from the stream
        /// </summary>
        /// <param name="stream">The stream from which to read the data</param>
        /// <returns>The serialized object in byte array form</returns>
        public override StreamingNsMessage<T> Decode(StreamWithPosition stream)
        {
            if (stream.CodecStream.Position != StreamWithPosition.CURRENTPOSITION)
            {
                stream.CodecStream.Position = stream.Position;
            }

            var avroSerializer = AvroSerializer.Create<StreamingNsMessage<T>>();
            StreamingNsMessage<T> message = avroSerializer.Deserialize(stream.CodecStream);

            message.Data = new List<T>();

            for (int i = 0; i < message.MessageCount; i++)
            {
                message.Data.Add(_codec.Decode(new StreamWithPosition(stream.CodecStream, stream.Position)));
            }

            return message;
        }

        /// <summary>
        /// Serialize the GroupCommunicationObject into a byte array using Protobuf.
        /// </summary>
        /// <param name="obj">The object to serialize.</param>
        /// <returns>The serialized GroupCommunicationMessage in byte array form</returns>
        public override byte[] Encode(StreamingNsMessage<T> obj)
        {
            MemoryStream stream = new MemoryStream();
            Encode(obj, new StreamWithPosition(stream, StreamWithPosition.CURRENTPOSITION));
            return stream.ToArray();
        }

        /// <summary>
        /// Deserialize the byte array into a NsMessage.
        /// </summary>
        /// <param name="data">The byte array to deserialize</param>
        /// <returns>The deserialized NsMessage object.</returns>
        public override StreamingNsMessage<T> Decode(byte[] data)
        {
            MemoryStream memStream = new MemoryStream(data);
            return Decode(new StreamWithPosition(memStream, StreamWithPosition.CURRENTPOSITION));
        }

    }
}
