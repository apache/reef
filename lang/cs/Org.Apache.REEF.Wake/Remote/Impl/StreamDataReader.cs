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
    /// <summary>
    /// Class with functions to Read from stream
    /// </summary>
    public sealed class StreamDataReader : IDataReader
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(StreamDataReader));
        
        /// <summary>
        /// Stream from which to read
        /// </summary>
        private readonly Stream _stream;
        
        /// <summary>
        /// Constructs the StreamDataReader
        /// </summary>
        /// <param name="stream">Stream from which to read</param>
        public StreamDataReader(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException("stream", "input stream cannot be null");
            }

            _stream = stream;
        }

        /// <summary>
        /// Reads double
        /// </summary>
        /// <returns>read double</returns>
        public double ReadDouble()
        {
            byte[] doubleBytes = new byte[sizeof(double)];
            int readBytes = Read(ref doubleBytes, 0, sizeof(double));

            if (readBytes == -1)
            {
                Exceptions.Throw(new Exception("No bytes read"), Logger);
            }
            return BitConverter.ToDouble(doubleBytes, 0);
        }

        /// <summary>
        /// Reads float
        /// </summary>
        /// <returns>read float</returns>
        public float ReadFloat()
        {
            byte[] floatBytes = new byte[sizeof(float)];
            int readBytes = Read(ref floatBytes, 0, sizeof(float));

            if (readBytes == -1)
            {
                Exceptions.Throw(new Exception("No bytes read"), Logger);
            }
            return BitConverter.ToSingle(floatBytes, 0);
        }

        /// <summary>
        /// Reads long
        /// </summary>
        /// <returns>read long</returns>
        public long ReadLong()
        {
            byte[] longBytes = new byte[sizeof(long)];
            int readBytes = Read(ref longBytes, 0, sizeof(long));

            if (readBytes == -1)
            {
                Exceptions.Throw(new Exception("No bytes read"), Logger);
            }
            return BitConverter.ToInt64(longBytes, 0);
        }

        /// <summary>
        /// Reads bool
        /// </summary>
        /// <returns>read bool</returns>
        public bool ReadBoolean()
        {
            byte[] boolBytes = new byte[sizeof(bool)];
            int readBytes = Read(ref boolBytes, 0, sizeof(bool));

            if (readBytes == -1)
            {
                Exceptions.Throw(new Exception("No bytes read"), Logger);
            }
            return BitConverter.ToBoolean(boolBytes, 0);
        }

        /// <summary>
        /// Reads integer
        /// </summary>
        /// <returns>read integer</returns>
        public int ReadInt32()
        {
            byte[] intBytes = new byte[sizeof(int)];
            int readBytes = Read(ref intBytes, 0, sizeof(int));

            if (readBytes == -1)
            {
                Exceptions.Throw(new Exception("No bytes read"), Logger);
            }

            return BitConverter.ToInt32(intBytes, 0);
        }

        /// <summary>
        /// Reads short
        /// </summary>
        /// <returns>read short</returns>
        public short ReadInt16()
        {
            byte[] intBytes = new byte[sizeof(short)];
            int readBytes = Read(ref intBytes, 0, sizeof(short));

            if (readBytes == -1)
            {
                Exceptions.Throw(new Exception("No bytes read"), Logger);
            }
            return BitConverter.ToInt16(intBytes, 0);
        }

        /// <summary>
        /// Reads string
        /// </summary>
        /// <returns>read string</returns>
        public string ReadString()
        {
            int length = ReadInt32();

            byte[] stringByte = new byte[length];
            int readBytes = Read(ref stringByte, 0, stringByte.Length);

            if (readBytes == -1)
            {
                return null;
            }

            char[] stringChar = new char[stringByte.Length / sizeof(char)];
            Buffer.BlockCopy(stringByte, 0, stringChar, 0, stringByte.Length);
            return new string(stringChar);
        }

        /// <summary>
        /// Reads data in to the buffer
        /// </summary>
        /// <param name="buffer">byte array to which to write</param>
        /// <param name="index">starting index in the array</param>
        /// <param name="bytesToRead">number of bytes to write</param>
        /// <returns>Task handler that reads bytes and returns that number of bytes read 
        /// if success, otherwise -1</returns>
        public int Read(ref byte[] buffer, int index, int bytesToRead)
        {
            if (buffer == null || buffer.Length < bytesToRead)
            {
                buffer = new byte[bytesToRead];
            }

            int totalBytesRead = 0;
            while (totalBytesRead < bytesToRead)
            {
                int bytesRead = _stream.Read(buffer, index + totalBytesRead, bytesToRead - totalBytesRead);
                if (bytesRead == 0)
                {
                    // Read timed out or connection was closed
                    return -1;
                }

                totalBytesRead += bytesRead;
            }

            return totalBytesRead;
        }

        /// <summary>
        /// Reads double asynchronously
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task handler that reads double</returns>
        public async Task<double> ReadDoubleAsync(CancellationToken token)
        {
            byte[] boolBytes = new byte[sizeof(double)];
            int readBytes = await ReadAsync(boolBytes, 0, sizeof(double), token);

            if (readBytes == -1)
            {
                Exceptions.Throw(new Exception("No bytes read"), Logger);
            }
            return BitConverter.ToDouble(boolBytes, 0);
        }

        /// <summary>
        /// Reads float asynchronously
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task handler that reads float</returns>
        public async Task<float> ReadFloatAsync(CancellationToken token)
        {
            byte[] boolBytes = new byte[sizeof(float)];
            int readBytes = await ReadAsync(boolBytes, 0, sizeof(float), token);

            if (readBytes == -1)
            {
                Exceptions.Throw(new Exception("No bytes read"), Logger);
            }
            return BitConverter.ToSingle(boolBytes, 0);
        }

        /// <summary>
        /// Reads long asynchronously
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task handler that reads long</returns>
        public async Task<long> ReadLongAsync(CancellationToken token)
        {
            byte[] longBytes = new byte[sizeof(long)];
            int readBytes = await ReadAsync(longBytes, 0, sizeof(long), token);

            if (readBytes == -1)
            {
                Exceptions.Throw(new Exception("No bytes read"), Logger);
            }
            return BitConverter.ToInt64(longBytes, 0);
        }

        /// <summary>
        /// Reads bool asynchronously
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task handler that reads bool</returns>
        public async Task<bool> ReadBooleanAsync(CancellationToken token)
        {
            byte[] boolBytes = new byte[sizeof(bool)];
            int readBytes = await ReadAsync(boolBytes, 0, sizeof(bool), token);

            if (readBytes == -1)
            {
                Exceptions.Throw(new Exception("No bytes read"), Logger);
            }
            return BitConverter.ToBoolean(boolBytes, 0);
        }

        /// <summary>
        /// Reads integer asynchronously
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task handler that reads integer</returns>
        public async Task<int> ReadInt32Async(CancellationToken token)
        {
            byte[] intBytes = new byte[sizeof(int)];
            int readBytes = await ReadAsync(intBytes, 0, sizeof(int), token);

            if (readBytes == -1)
            {
                Exceptions.Throw(new Exception("No bytes read"), Logger);
            }
            return BitConverter.ToInt32(intBytes, 0);
        }

        /// <summary>
        /// Reads short asynchronously
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task handler that reads short</returns>
        public async Task<short> ReadInt16Async(CancellationToken token)
        {
            byte[] intBytes = new byte[sizeof(short)];
            int readBytes = await ReadAsync(intBytes, 0, sizeof(short), token);

            if (readBytes == -1)
            {
                Exceptions.Throw(new Exception("No bytes read"), Logger);
            }
            return BitConverter.ToInt16(intBytes, 0);
        }

        /// <summary>
        /// Reads string asynchronously
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task handler that reads string</returns>
        public async Task<string> ReadStringAsync(CancellationToken token)
        {
            int length = ReadInt32();

            byte[] stringByte = new byte[length];
            int readBytes = await ReadAsync(stringByte, 0, stringByte.Length, token);

            if (readBytes == -1)
            {
                return null;
            }

            char[] stringChar = new char[stringByte.Length / sizeof(char)];
            Buffer.BlockCopy(stringByte, 0, stringChar, 0, stringByte.Length);
            return new string(stringChar);
        }

        /// <summary>
        /// Reads data in to the buffer asynchronously
        /// </summary>
        /// <param name="buffer">byte array to which to write</param>
        /// <param name="index">starting index in the array</param>
        /// <param name="bytesToRead">number of bytes to write</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task handler that reads bytes and returns that number of bytes read 
        /// if success, otherwise -1</returns>
        public async Task<int> ReadAsync(byte[] buffer, int index, int bytesToRead, CancellationToken token)
        {
            int totalBytesRead = 0;
            while (totalBytesRead < bytesToRead)
            {
                int bytesRead = await _stream.ReadAsync(buffer, totalBytesRead + index, bytesToRead - totalBytesRead, token);
                if (bytesRead == 0)
                {
                    // Read timed out or connection was closed
                    return -1;
                }

                totalBytesRead += bytesRead;
            }

            return bytesToRead;
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
