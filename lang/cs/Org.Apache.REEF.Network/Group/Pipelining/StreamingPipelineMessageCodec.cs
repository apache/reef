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

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.Network.Group.Pipelining
{
    /// <summary>
    /// The streaming codec for PipelineMessage
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public sealed class StreamingPipelineMessageCodec<T> : IStreamingCodec<PipelineMessage<T>>
    {
        /// <summary>
        /// Creates new PipelineMessageCodec
        /// </summary>
        /// <param name="baseCodec">The codec for actual message in PipelineMessage</param>
        [Inject]
        private StreamingPipelineMessageCodec(IStreamingCodec<T> baseCodec)
        {
            BaseCodec = baseCodec;
        }
                
        /// <summary>
        /// Codec for actual message T
        /// </summary>
        public IStreamingCodec<T> BaseCodec { get; }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <returns>The Pipeline Message read from the reader</returns>
        public PipelineMessage<T> Read(IDataReader reader)
        {
            var message = BaseCodec.Read(reader);
            var isLast = reader.ReadBoolean();
            return new PipelineMessage<T>(message, isLast);
        }

        /// <summary>
        /// Writes the class fields to the writer.
        /// </summary>
        /// <param name="obj">The object to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        public void Write(PipelineMessage<T> obj, IDataWriter writer)
        {
            BaseCodec.Write(obj.Data, writer);
            writer.WriteBoolean(obj.IsLast);
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>The Pipeline Message  read from the reader</returns>
        public async Task<PipelineMessage<T>> ReadAsync(IDataReader reader, CancellationToken token)
        {
            var message = await BaseCodec.ReadAsync(reader, token);
            var isLast = await reader.ReadBooleanAsync(token);
            return new PipelineMessage<T>(message, isLast);
        }

        /// <summary>
        /// Writes the class fields to the writer.
        /// </summary>
        /// <param name="obj">The object to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">Cancellation token</param>
        public async System.Threading.Tasks.Task WriteAsync(PipelineMessage<T> obj, IDataWriter writer, CancellationToken token)
        {
            await BaseCodec.WriteAsync(obj.Data, writer, token);
            await writer.WriteBooleanAsync(obj.IsLast, token);
        }
    }
}
