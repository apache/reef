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
using System.Text;
using System.Threading.Tasks;
using Org.Apache.Reef.IO.Network.Group.Codec;
using Org.Apache.Reef.IO.Network.Group.Driver;
using Org.Apache.Reef.IO.Network.Group.Driver.Impl;
using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Wake.Remote;
using ProtoBuf;

namespace Org.Apache.Reef.IO.Network.Group.Codec
{
    /// <summary>
    /// Used to serialize GroupCommunicationMessages.
    /// </summary>
    public class GroupCommunicationMessageCodec : ICodec<GroupCommunicationMessage>
    {
        /// <summary>
        /// Create a new GroupCommunicationMessageCodec.
        /// </summary>
        [Inject]
        public GroupCommunicationMessageCodec()
        {
        }

        /// <summary>
        /// Serialize the GroupCommunicationObject into a byte array using Protobuf.
        /// </summary>
        /// <param name="obj">The object to serialize.</param>
        /// <returns>The serialized GroupCommunicationMessage in byte array form</returns>
        public byte[] Encode(GroupCommunicationMessage obj)
        {
            GcmMessageProto proto = GcmMessageProto.Create(obj);
            using (var stream = new MemoryStream())
            {
                Serializer.Serialize(stream, proto);
                return stream.ToArray();
            }
        }

        /// <summary>
        /// Deserialize the byte array into a GroupCommunicationMessage using Protobuf.
        /// </summary>
        /// <param name="data">The byte array to deserialize</param>
        /// <returns>The deserialized GroupCommunicationMessage object.</returns>
        public GroupCommunicationMessage Decode(byte[] data)
        {
            using (var stream = new MemoryStream(data))
            {
                GcmMessageProto proto = Serializer.Deserialize<GcmMessageProto>(stream);
                return proto.ToGcm();
            }
        }
    }
}
