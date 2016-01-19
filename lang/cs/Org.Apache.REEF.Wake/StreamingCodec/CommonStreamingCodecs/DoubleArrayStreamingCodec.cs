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

using System;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs
{
    /// <summary>
    /// Streaming codec for double array
    /// </summary>
    public sealed class DoubleArrayStreamingCodec : IStreamingCodec<double[]>
    {
        /// <summary>
        /// Injectable constructor
        /// </summary>
        [Inject]
        private DoubleArrayStreamingCodec()
        {
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <returns>The double array read from the reader</returns>
        public double[] Read(IDataReader reader)
        {
            int length = reader.ReadInt32();
            byte[] buffer = new byte[sizeof(double) * length];
            reader.Read(ref buffer, 0, buffer.Length);
            double[] doubleArr = new double[length];
            Buffer.BlockCopy(buffer, 0, doubleArr, 0, buffer.Length);
            return doubleArr;
        }

        /// <summary>
        /// Writes the double array to the writer.
        /// </summary>
        /// <param name="obj">The double array to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        public void Write(double[] obj, IDataWriter writer)
        {
            if (obj == null)
            {
                throw new ArgumentNullException("obj", "double array is null");
            }

            writer.WriteInt32(obj.Length);
            byte[] buffer = new byte[sizeof(double) * obj.Length];
            Buffer.BlockCopy(obj, 0, buffer, 0, buffer.Length);
            writer.Write(buffer, 0, buffer.Length);
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>The double array read from the reader</returns>
        public async Task<double[]> ReadAsync(IDataReader reader, CancellationToken token)
        {
            int length = await reader.ReadInt32Async(token);
            byte[] buffer = new byte[sizeof(double) * length];
            await reader.ReadAsync(buffer, 0, buffer.Length, token);
            double[] doubleArr = new double[length];
            Buffer.BlockCopy(buffer, 0, doubleArr, 0, sizeof(double) * length);
            return doubleArr;
        }

        /// <summary>
        /// Writes the double array to the writer.
        /// </summary>
        /// <param name="obj">The double array to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">Cancellation token</param>
        public async Task WriteAsync(double[] obj, IDataWriter writer, CancellationToken token)
        {
            if (obj == null)
            {
                throw new ArgumentNullException("obj", "double array is null");
            }

            await writer.WriteInt32Async(obj.Length, token);
            byte[] buffer = new byte[sizeof(double) * obj.Length];
            Buffer.BlockCopy(obj, 0, buffer, 0, sizeof(double) * obj.Length);
            await writer.WriteAsync(buffer, 0, buffer.Length, token);
        }
    }
}
