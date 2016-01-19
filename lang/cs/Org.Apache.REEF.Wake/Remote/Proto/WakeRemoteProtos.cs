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
using ProtoBuf;

namespace Org.Apache.REEF.Wake.Remote.Proto
{
    /// <summary>
    /// Message p buff
    /// </summary>
    public partial class WakeMessagePBuf
    {
        public static WakeMessagePBuf Deserialize(byte[] bytes)
        {
            WakeMessagePBuf pbuf = null;
            using (var s = new MemoryStream(bytes))
            {
                pbuf = Serializer.Deserialize<WakeMessagePBuf>(s);
            }
            return pbuf;
        }

        public byte[] Serialize()
        {
            using (var s = new MemoryStream())
            {
                Serializer.Serialize(s, this);
                return s.ToArray();
            }
        }
    }

    /// <summary>
    /// Wake tuple buf
    /// </summary>
    public partial class WakeTuplePBuf
    {
        public static WakeTuplePBuf Deserialize(byte[] bytes)
        {
            WakeTuplePBuf pbuf = null;
            using (var s = new MemoryStream(bytes))
            {
                pbuf = Serializer.Deserialize<WakeTuplePBuf>(s);
            }
            return pbuf;
        }

        public byte[] Serialize()
        {
            using (var s = new MemoryStream())
            {
                Serializer.Serialize(s, this);
                return s.ToArray();
            }
        }
    }
}
