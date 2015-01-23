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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.Reef.IO.Network.Group.Driver;
using Org.Apache.Reef.IO.Network.Group.Driver.Impl;
using ProtoBuf;

namespace Org.Apache.Reef.IO.Network.Group.Codec
{
    [ProtoContract]
    public class GcmMessageProto
    {
        [ProtoMember(1)]
        public byte[][] Data { get; set; }

        [ProtoMember(2)]
        public string OperatorName { get; set; }

        [ProtoMember(3)]
        public string GroupName { get; set; }

        [ProtoMember(4)]
        public string Source { get; set; }

        [ProtoMember(5)]
        public string Destination { get; set; }

        [ProtoMember(6)]
        public MessageType MsgType { get; set; }

        public static GcmMessageProto Create(GroupCommunicationMessage gcm)
        {
            return new GcmMessageProto()
            {
                Data = gcm.Data,
                OperatorName = gcm.OperatorName,
                GroupName = gcm.GroupName,
                Source = gcm.Source,
                Destination = gcm.Destination,
                MsgType = gcm.MsgType,
            };
        }

        public GroupCommunicationMessage ToGcm()
        {
            return new GroupCommunicationMessage(
                GroupName,
                OperatorName,
                Source,
                Destination,
                Data,
                MsgType);
        }
    }
}