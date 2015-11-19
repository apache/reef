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

using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Writable remote event class
    /// </summary>
    /// <typeparam name="T">Type of remote event message. It is assumed that T implements IWritable</typeparam>
    internal sealed class RemoteEventStreamingCodec<T> : IStreamingCodec<IRemoteEvent<T>>
    {
        private readonly IStreamingCodec<T> _codec;

        internal RemoteEventStreamingCodec(IStreamingCodec<T> codec)
        {
            _codec = codec;
        } 
        
        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        /// <returns>The remote event</returns>
        public IRemoteEvent<T> Read(IDataReader reader)
        {
            return new RemoteEvent<T>(null, null, _codec.Read(reader));
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="value">The remote event</param>
        /// <param name="writer">The writer to which to write</param>
        public void Write(IRemoteEvent<T> value, IDataWriter writer)
        {
            _codec.Write(value.Value, writer);
        }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        /// <param name="token">The cancellation token</param>
        /// <returns>The remote event</returns>
        public async Task<IRemoteEvent<T>> ReadAsync(IDataReader reader, CancellationToken token)
        {
            T message = await _codec.ReadAsync(reader, token);
            return new RemoteEvent<T>(null, null, message);     
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="value">The remote event</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">The cancellation token</param>
        public async Task WriteAsync(IRemoteEvent<T> value, IDataWriter writer, CancellationToken token)
        {
            await _codec.WriteAsync(value.Value, writer, token);        
        }
    }
}
