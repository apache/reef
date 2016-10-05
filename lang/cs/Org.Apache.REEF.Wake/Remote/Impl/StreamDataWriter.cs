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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    public sealed class StreamDataWriter : IDataWriter
    {
        Logger Logger = Logger.GetLogger(typeof(StreamDataWriter));

         /// <summary>
        /// Stream to which to write
        /// </summary>
        private readonly Stream _stream;
        
        /// <summary>
        /// Constructs the StreamDataReader
        /// </summary>
        /// <param name="stream">Stream from which to read</param>
        public StreamDataWriter(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream", "input stream cannot be null");
            }

            _stream = stream;
        }

        /// <summary>
        /// write double
        /// </summary>
        /// <param name="obj">double to be written</param>
        public void WriteDouble(double obj)
        {
            _stream.Write(BitConverter.GetBytes(obj), 0, sizeof(double));
        }

        /// <summary>
        /// write float
        /// </summary>
        /// <param name="obj">float to be written</param>
        public void WriteFloat(float obj)
        {
            _stream.Write(BitConverter.GetBytes(obj), 0, sizeof(float));
        }

        /// <summary>
        /// write long
        /// </summary>
        /// <param name="obj">long to be written</param>
        public void WriteLong(long obj)
        {
            _stream.Write(BitConverter.GetBytes(obj), 0, sizeof(long));
        }

        /// <summary>
        /// write boolean
        /// </summary>
        /// <param name="obj">bool to be written</param>
        public void WriteBoolean(bool obj)
        {
            _stream.Write(BitConverter.GetBytes(obj), 0, sizeof(bool));
        }

        /// <summary>
        /// write integer
        /// </summary>
        /// <param name="obj">int to be written</param>
        public void WriteInt32(int obj)
        {
            _stream.Write(BitConverter.GetBytes(obj), 0, sizeof(int));
        }

        /// <summary>
        /// write short
        /// </summary>
        /// <param name="obj">short to be written</param>
        public void WriteInt16(short obj)
        {
            _stream.Write(BitConverter.GetBytes(obj), 0, sizeof(short));
        }

        /// <summary>
        /// write string
        /// </summary>
        /// <param name="obj">string to be written</param>
        public void WriteString(string obj)
        {
            var charString = obj.ToCharArray();
            byte[] byteString = new byte[charString.Length * sizeof(char)];
            WriteInt32(byteString.Length);
            Buffer.BlockCopy(charString, 0, byteString, 0, byteString.Length);
            _stream.Write(byteString, 0, byteString.Length);
        }

        /// <summary>
        /// write bytes to the byte array
        /// </summary>
        /// <param name="buffer">byte array from which to read</param>
        /// <param name="index">starting index in the array</param>
        /// <param name="count">number of bytes to write</param>
        public void Write(byte[] buffer, int index, int count)
        {
            _stream.Write(buffer, index, count);
        }

        /// <summary>
        /// write double asynchronously
        /// </summary>
        /// <param name="obj">double to be written</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>the handler to the task</returns>
        public async Task WriteDoubleAsync(double obj, CancellationToken token)
        {
            await _stream.WriteAsync(BitConverter.GetBytes(obj), 0, sizeof(double), token);
        }

        /// <summary>
        /// write float asynchronously
        /// </summary>
        /// <param name="obj">float to be written</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>the handler to the task</returns>
        public async Task WriteFloatAsync(float obj, CancellationToken token)
        {
            await _stream.WriteAsync(BitConverter.GetBytes(obj), 0, sizeof(float), token);
        }

        /// <summary>
        /// write long asynchronously
        /// </summary>
        /// <param name="obj">long to be written</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>the handler to the task</returns>
        public async Task WriteLongAsync(long obj, CancellationToken token)
        {
            await _stream.WriteAsync(BitConverter.GetBytes(obj), 0, sizeof(long), token);
        }

        /// <summary>
        /// write bool asynchronously
        /// </summary>
        /// <param name="obj">bool to be written</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>the handler to the task</returns>
        public async Task WriteBooleanAsync(bool obj, CancellationToken token)
        {
            await _stream.WriteAsync(BitConverter.GetBytes(obj), 0, sizeof(bool), token);
        }

        /// <summary>
        /// write integer asynchronously
        /// </summary>
        /// <param name="obj">integer to be written</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>the handler to the task</returns>
        public async Task WriteInt32Async(int obj, CancellationToken token)
        {
            await _stream.WriteAsync(BitConverter.GetBytes(obj), 0, sizeof(int), token);
        }

        /// <summary>
        /// write short asynchronously
        /// </summary>
        /// <param name="obj">short to be written</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>the handler to the task</returns>
        public async Task WriteInt16Async(short obj, CancellationToken token)
        {
            await _stream.WriteAsync(BitConverter.GetBytes(obj), 0, sizeof(short), token);
        }

        /// <summary>
        /// write string asynchronously
        /// </summary>
        /// <param name="obj">string to be written</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>the handler to the task</returns>
        public async Task WriteStringAsync(string obj, CancellationToken token)
        {
            var charString = obj.ToCharArray();
            byte[] byteString = new byte[charString.Length * sizeof(char)];
            await WriteInt32Async(byteString.Length, token);
            Buffer.BlockCopy(charString, 0, byteString, 0, byteString.Length);
            await _stream.WriteAsync(byteString, 0, byteString.Length, token);
        }

        /// <summary>
        /// write bytes to the byte array asynchronously
        /// </summary>
        /// <param name="buffer">byte array from which to read</param>
        /// <param name="index">starting index in the array</param>
        /// <param name="count">number of bytes to write</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>the handler to the task</returns>
        public async Task WriteAsync(byte[] buffer, int index, int count, CancellationToken token)
        {
            await _stream.WriteAsync(buffer, index, count, token);
        }

        /// <summary>
        /// Gets underlying stream. Throws null exception if 
        /// stream is null or not available
        /// </summary>
        /// <returns>The underlying stream</returns>
        public Stream GetStream()
        {
            if (_stream == null)
            {
                Exceptions.Throw(new NullReferenceException("Stream is null or not available"), Logger);
            }
            return _stream;
        }
    }
}
