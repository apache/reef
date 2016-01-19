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

using System.IO;
using System.Linq;
using Org.Apache.REEF.Wake;
using Org.Apache.REEF.Wake.Remote;
using ProtoBuf;

namespace Org.Apache.REEF.Network.NetworkService.Codec
{
    /// <summary>
    /// Codec to serialize NsMessages for NetworkService.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public class NsMessageCodec<T> : ICodec<NsMessage<T>>
    {
        private readonly ICodec<T> _codec;
        private readonly IIdentifierFactory _idFactory;

        /// <summary>
        /// Create new NsMessageCodec.
        /// </summary>
        /// <param name="codec">The codec used to serialize message data</param>
        /// <param name="idFactory">Used to create identifier from string.</param>
        public NsMessageCodec(ICodec<T> codec, IIdentifierFactory idFactory)
        {
            _codec = codec;
            _idFactory = idFactory;
        }

        /// <summary>
        /// Serialize the NsMessage.
        /// </summary>
        /// <param name="obj">The object to serialize</param>
        /// <returns>The serialized object in byte array form</returns>
        public byte[] Encode(NsMessage<T> obj)
        {
            NsMessageProto proto = NsMessageProto.Create(obj, _codec);
            using (var stream = new MemoryStream())
            {
                Serializer.Serialize(stream, proto);
                return stream.ToArray();
            }
        }

        /// <summary>
        /// Deserialize the byte array into NsMessage.
        /// </summary>
        /// <param name="data">The serialized byte array</param>
        /// <returns>The deserialized NsMessage</returns>
        public NsMessage<T> Decode(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            {
                NsMessageProto proto = Serializer.Deserialize<NsMessageProto>(stream);

                IIdentifier sourceId = _idFactory.Create(proto.SourceId);
                IIdentifier destId = _idFactory.Create(proto.DestId);
                NsMessage<T> message = new NsMessage<T>(sourceId, destId);

                var messages = proto.Data.Select(byteArr => _codec.Decode(byteArr));
                message.Data.AddRange(messages);
                return message;
            }
        }
    }
}
