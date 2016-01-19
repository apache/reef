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
    /// Streaming codec for double
    /// </summary>
    public sealed class DoubleStreamingCodec : IStreamingCodec<double>
    {
        /// <summary>
        /// Injectable constructor
        /// </summary>
        [Inject]
        private DoubleStreamingCodec()
        {
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <returns>The double read from the reader</returns>
        public double Read(IDataReader reader)
        {
            return reader.ReadDouble();
        }

        /// <summary>
        /// Writes the double to the writer.
        /// </summary>
        /// <param name="obj">The double to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        public void Write(double obj, IDataWriter writer)
        {
            writer.WriteDouble(obj);
        }

        /// <summary>
        /// Instantiate the class from the reader.
        /// </summary>
        /// <param name="reader">The reader from which to read</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>The double read from the reader</returns>
        public async Task<double> ReadAsync(IDataReader reader, CancellationToken token)
        {
            return await reader.ReadDoubleAsync(token);
        }

        /// <summary>
        /// Writes the double to the writer.
        /// </summary>
        /// <param name="obj">The double to be encoded</param>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">Cancellation token</param>
        public async Task WriteAsync(double obj, IDataWriter writer, CancellationToken token)
        {
            await writer.WriteDoubleAsync(obj, token);
        }
    }
}
