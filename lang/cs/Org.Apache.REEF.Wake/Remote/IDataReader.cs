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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Wake.Remote
{
    /// <summary>
    /// Interface for reading standard primitives
    /// </summary>
    public interface IDataReader
    {
        /// <summary>
        /// Reads double
        /// </summary>
        /// <returns>read double</returns>
        double ReadDouble();

        /// <summary>
        /// Reads float
        /// </summary>
        /// <returns>read float</returns>
        float ReadFloat();

        /// <summary>
        /// Reads long
        /// </summary>
        /// <returns>read long</returns>
        long ReadLong();

        /// <summary>
        /// Reads bool
        /// </summary>
        /// <returns>read bool</returns>
        bool ReadBoolean();

        /// <summary>
        /// Reads integer
        /// </summary>
        /// <returns>read integer</returns>
        int ReadInt32();

        /// <summary>
        /// Reads short
        /// </summary>
        /// <returns>read short</returns>
        short ReadInt16();

        /// <summary>
        /// Reads string
        /// </summary>
        /// <returns>read string</returns>
        string ReadString();

        /// <summary>
        /// Reads data in to the buffer
        /// </summary>
        /// <param name="buffer">byte array to which to write</param>
        /// <param name="index">starting index in the array</param>
        /// <param name="bytesToRead">number of bytes to write</param>
        /// <returns>Task handler that reads bytes and returns that number of bytes read 
        /// if success, otherwise -1</returns>
        int Read(ref byte[] buffer, int index, int bytesToRead);

        /// <summary>
        /// Reads double asynchronously
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task handler that reads double</returns>
        Task<double> ReadDoubleAsync(CancellationToken token);

        /// <summary>
        /// Reads float asynchronously
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task handler that reads float</returns>
        Task<float> ReadFloatAsync(CancellationToken token);

        /// <summary>
        /// Reads long asynchronously
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task handler that reads long</returns>
        Task<long> ReadLongAsync(CancellationToken token);

        /// <summary>
        /// Reads bool asynchronously
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task handler that reads bool</returns>
        Task<bool> ReadBooleanAsync(CancellationToken token);

        /// <summary>
        /// Reads integer asynchronously
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task handler that reads integer</returns>
        Task<int> ReadInt32Async(CancellationToken token);

        /// <summary>
        /// Reads short asynchronously
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task handler that reads short</returns>
        Task<short> ReadInt16Async(CancellationToken token);

        /// <summary>
        /// Reads string asynchronously
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task handler that reads string</returns>
        Task<string> ReadStringAsync(CancellationToken token);

        /// <summary>
        /// Reads data in to the buffer asynchronously
        /// </summary>
        /// <param name="buffer">byte array to which to write</param>
        /// <param name="index">starting index in the array</param>
        /// <param name="bytesToRead">number of bytes to write</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>Task handler that reads bytes and returns that number of bytes read 
        /// if success, otherwise -1</returns>
        Task<int> ReadAsync(byte[] buffer, int index, int bytesToRead, CancellationToken token);

        /// <summary>
        /// Gets underlying stream. Throws null exception if 
        /// stream is null or not available
        /// </summary>
        /// <returns>The underlying stream</returns>
        Stream GetStream();
    }
}
