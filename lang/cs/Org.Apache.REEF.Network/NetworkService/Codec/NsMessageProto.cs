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

using System.Collections.Generic;
using Org.Apache.REEF.Wake.Remote;
using ProtoBuf;

namespace Org.Apache.REEF.Network.NetworkService.Codec
{
    [ProtoContract]
    public class NsMessageProto
    {
        public NsMessageProto()
        {
            Data = new List<byte[]>(); 
        }

        [ProtoMember(1)]
        public string SourceId { get; set; }

        [ProtoMember(2)]
        public string DestId { get; set; }

        [ProtoMember(3)]
        public List<byte[]> Data { get; set; } 

        public static NsMessageProto Create<T>(NsMessage<T> message, ICodec<T> codec)
        {
            NsMessageProto proto = new NsMessageProto();

            proto.SourceId = message.SourceId.ToString();
            proto.DestId = message.DestId.ToString();

            foreach (T item in message.Data)
            {
                proto.Data.Add(codec.Encode(item));
            }

            return proto;
        }
    }
}
