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

namespace Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs
{
    /// <summary>
    /// Streaming codec for integer
    /// </summary>
    public sealed class IntStreamingCodec : IStreamingCodec<int>
    {
        /// <summary>
        /// Injectable constructor
        /// </summary>
        [Inject]
        private IntStreamingCodec()
        {
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <returns>The integer read from the reader</returns>
        public int Read(IDataReader reader)
        {
            return reader.ReadInt32();
        }

        /// <summary>
        /// Writes the integer to the writer.
        /// </summary>
        /// <param name="obj">The integer to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        public void Write(int obj, IDataWriter writer)
        {
            writer.WriteInt32(obj);
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>The integer read from the reader</returns>
        public async Task<int> ReadAsync(IDataReader reader, CancellationToken token)
        {
            return await reader.ReadInt32Async(token);
        }

        /// <summary>
        /// Writes the integer to the writer.
        /// </summary>
        /// <param name="obj">The integer to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">Cancellation token</param>
        public async Task WriteAsync(int obj, IDataWriter writer, CancellationToken token)
        {
            await writer.WriteInt32Async(obj, token);
        }
    }
}
