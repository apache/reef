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
using System.ComponentModel;
using System.IO;
using System.Linq;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.StreamingCodec;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;
using ProtoBuf;
using Microsoft.Hadoop.Avro;
using Org.Apache.REEF.Network.StreamingCodec.Impl;

namespace Org.Apache.REEF.Network.Group.Codec
{
    /// <summary>
    /// Used to serialize GroupCommunicationMessages. This is the StreamingCodec for 
    /// Group Communication Message. The base data codec in DataPlusCodec can either 
    /// be streaming or non-streaming.
    /// </summary>
    public class StreamingGroupCommunicationMessageCodec : StreamingDataCodec<StreamingGroupCommunicationMessage>
    {
        /// <summary>
        /// Create a new GroupCommunicationMessageCodec.
        /// </summary>
        [Inject]
        public StreamingGroupCommunicationMessageCodec()
        {
        }

        /// <summary>
        /// Serialize the GroupCommunicationObject into a byte array.
        /// </summary>
        /// <param name="obj">The object to serialize.</param>
        /// <returns>The serialized GroupCommunicationMessage in byte array form</returns>
        public override byte[] Encode(StreamingGroupCommunicationMessage obj)
        {
            MemoryStream stream = new MemoryStream();
            Encode(obj, new StreamWithPosition(stream, 0));
            return stream.ToArray();
        }

        /// <summary>
        /// Deserialize the byte array into a GroupCommunicationMessage using Protobuf.
        /// </summary>
        /// <param name="data">The byte array to deserialize</param>
        /// <returns>The deserialized GroupCommunicationMessage object.</returns>
        public override StreamingGroupCommunicationMessage Decode(byte[] data)
        {
            MemoryStream memStream = new MemoryStream(data);
            return Decode(new StreamWithPosition(memStream, 0));
        }

        /// <summary>Encodes the given StreamingGroupCommunicatioMessage object and 
        /// write it to the stream. First all the fields except the data and codec are 
        /// encoded using Avro. Then data is written using the codec.</summary>
        /// <param name="obj"> the object to be encoded</param>
        /// <param name="stream"> stream we want the encoding written to</param>
        public override void Encode(StreamingGroupCommunicationMessage obj, StreamWithPosition stream)
        {
            var avroSerializer = AvroSerializer.Create<StreamingGroupCommunicationMessage>();
            avroSerializer.Serialize(stream.CodecStream, obj);

            if (obj.Data.Codec is IStreamingDataCodecObj)
            {
                IStreamingDataCodecObj codec = (IStreamingDataCodecObj) obj.Data.Codec;

                foreach (var data in obj.Data.Data)
                {
                    codec.EncodeObject(data, stream);                    
                }
            }
            else
            {
                foreach (var data in obj.Data.Data)
                {
                    var byteData = obj.Data.Codec.EncodeObject(data);
                    stream.CodecStream.Write(byteData, 0, byteData.Length);
                }
            }
        }

        /// <summary>Decode the data from the stream to the StreamingGroupCommunicationMessage.
        /// All the member fields excet data and codec in DataPlusCodec are decoded. Then the 
        /// reference to the stream is stored within the DataPlusCodec so that it 
        /// can be used to decode the actual data later in OperatorTopology.</summary>
        /// <param name="stream"> stream we want to decode from</param>
        /// <returns>The decoded object of type T</returns>
        public override StreamingGroupCommunicationMessage Decode(StreamWithPosition stream)
        {
            if (stream.CodecStream.Position != StreamWithPosition.CURRENTPOSITION)
            {
                stream.CodecStream.Position = stream.Position;
            }
           
            var avroSerializer = AvroSerializer.Create<StreamingGroupCommunicationMessage>();
            StreamingGroupCommunicationMessage message = avroSerializer.Deserialize(stream.CodecStream);
            message.Data.ReadableStream = new StreamWithPosition(stream.CodecStream, stream.Position);
            return message;
        }
    }
}
