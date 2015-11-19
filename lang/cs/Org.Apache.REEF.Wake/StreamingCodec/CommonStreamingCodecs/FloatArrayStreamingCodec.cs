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
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs
{
    /// <summary>
    /// Streaming codec for float array
    /// </summary>
    public sealed class FloatArrayStreamingCodec : IStreamingCodec<float[]>
    {
        /// <summary>
        /// Injectable constructor
        /// </summary>
        [Inject]
        private FloatArrayStreamingCodec()
        {
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <returns>The float array read from the reader</returns>
        public float[] Read(IDataReader reader)
        {
            int length = reader.ReadInt32();
            byte[] buffer = new byte[sizeof(float) * length];
            reader.Read(ref buffer, 0, buffer.Length);
            float[] floatArr = new float[length];
            Buffer.BlockCopy(buffer, 0, floatArr, 0, buffer.Length);
            return floatArr;
        }

        /// <summary>
        /// Writes the float array to the writer.
        /// </summary>
        /// <param name="obj">The float array to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        public void Write(float[] obj, IDataWriter writer)
        {
            if (obj == null)
            {
                throw new ArgumentNullException("obj", "float array is null");
            }

            writer.WriteInt32(obj.Length);
            byte[] buffer = new byte[sizeof(float) * obj.Length];
            Buffer.BlockCopy(obj, 0, buffer, 0, buffer.Length);
            writer.Write(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>The float array read from the reader</returns>
        public async Task<float[]> ReadAsync(IDataReader reader, CancellationToken token)
        {
            int length = await reader.ReadInt32Async(token);
            byte[] buffer = new byte[sizeof(float) * length];
            await reader.ReadAsync(buffer, 0, buffer.Length, token);
            float[] floatArr = new float[length];
            Buffer.BlockCopy(buffer, 0, floatArr, 0, sizeof(float) * length);
            return floatArr;
        }

        /// <summary>
        /// Writes the float array to the writer.
        /// </summary>
        /// <param name="obj">The float array to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">Cancellation token</param>
        public async Task WriteAsync(float[] obj, IDataWriter writer, CancellationToken token)
        {
            if (obj == null)
            {
                throw new ArgumentNullException("obj", "float array is null");
            }

            await writer.WriteInt32Async(obj.Length, token);
            byte[] buffer = new byte[sizeof(float) * obj.Length];
            Buffer.BlockCopy(obj, 0, buffer, 0, sizeof(float) * obj.Length);
            await writer.WriteAsync(buffer, 0, buffer.Length, token);
        }
    }
}
