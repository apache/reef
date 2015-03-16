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

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Remote;
using System;
using System.Linq;

namespace Org.Apache.REEF.Network.Group.Pipelining
{
    /// <summary>
    /// the message for pipelined communication
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public class PipelineMessageCodec<T> : ICodec<PipelineMessage<T>>
    {
        /// <summary>
        /// Create new PipelineMessage.
        /// </summary>
        /// <param name="data">The actual byte data</param>
        /// <param name="isLast">Whether this is last pipeline message</param>
        [Inject]
        public PipelineMessageCodec(ICodec<T> baseCodec)
        {
            BaseCodec = baseCodec;
        }

        public byte[] Encode(PipelineMessage<T> obj)
        {
            byte[] baseCoding = BaseCodec.Encode(obj.Data);
            byte[] result = new byte[baseCoding.Length + sizeof(bool)];
            Buffer.BlockCopy(baseCoding, 0, result, 0, baseCoding.Length);
            Buffer.BlockCopy(BitConverter.GetBytes(obj.IsLast), 0, result, baseCoding.Length, sizeof(bool));
            return result;
        }

        public PipelineMessage<T> Decode(byte[] data)
        {
            T message = BaseCodec.Decode(data.Take(data.Length - sizeof(bool)).ToArray());
            bool isLast = BitConverter.ToBoolean(data, data.Length - sizeof(bool));
            return new PipelineMessage<T>(message, isLast);
        }
        
        
        /// <summary>
        /// Codec for actual message T
        /// </summary>
        public ICodec<T> BaseCodec { get; private set; }
    }
}
