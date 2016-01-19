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

using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Wake.StreamingCodec
{
    /// <summary>
    /// Converts codec to streaming codec
    /// </summary>
    /// <typeparam name="T">Message type</typeparam>
    public sealed class CodecToStreamingCodec<T> : IStreamingCodec<T>
    {
        private readonly ICodec<T> _codec;
            
        [Inject]
        private CodecToStreamingCodec(ICodec<T> codec)
        {
            _codec = codec;
        }

        public T Read(IDataReader reader)
        {
            int length = reader.ReadInt32();
            byte[] byteArr = new byte[length];
            reader.Read(ref byteArr, 0, length);
            return _codec.Decode(byteArr);
        }

        public void Write(T obj, IDataWriter writer)
        {
            var byteArr = _codec.Encode(obj);
            writer.WriteInt32(byteArr.Length);
            writer.Write(byteArr, 0, byteArr.Length);
        }

        public async Task<T> ReadAsync(IDataReader reader, CancellationToken token)
        {
            int length = await reader.ReadInt32Async(token);
            byte[] byteArr = new byte[length];
            await reader.ReadAsync(byteArr, 0, length, token);
            return _codec.Decode(byteArr);
        }

        public async Task WriteAsync(T obj, IDataWriter writer, CancellationToken token)
        {
            var byteArr = _codec.Encode(obj);
            await writer.WriteInt32Async(byteArr.Length, token);
            await writer.WriteAsync(byteArr, 0, byteArr.Length, token);
        }
    }
}
