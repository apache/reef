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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Wake.Remote
{
    /// <summary>
    /// Interface for writing standard primitives
    /// </summary>
    public interface IDataWriter
    {
        /// <summary>
        /// write double
        /// </summary>
        /// <param name="obj">double to be written</param>
        void WriteDouble(double obj);

        /// <summary>
        /// write float
        /// </summary>
        /// <param name="obj">float to be written</param>
        void WriteFloat(float obj);

        /// <summary>
        /// write long
        /// </summary>
        /// <param name="obj">long to be written</param>
        void WriteLong(long obj);

        /// <summary>
        /// write boolean
        /// </summary>
        /// <param name="obj">bool to be written</param>
        void WriteBoolean(bool obj);

        /// <summary>
        /// write integer
        /// </summary>
        /// <param name="obj">int to be written</param>
        void WriteInt32(int obj);

        /// <summary>
        /// write short
        /// </summary>
        /// <param name="obj">short to be written</param>
        void WriteInt16(short obj);

        /// <summary>
        /// write string
        /// </summary>
        /// <param name="obj">string to be written</param>
        void WriteString(string obj);

        /// <summary>
        /// write bytes to the byte array
        /// </summary>
        /// <param name="buffer">byte array from which to read</param>
        /// <param name="index">starting index in the array</param>
        /// <param name="count">number of bytes to write</param>
        void Write(byte[] buffer, int index, int count);

        /// <summary>
        /// write double asynchronously
        /// </summary>
        /// <param name="obj">double to be written</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>the handler to the task</returns>
        Task WriteDoubleAsync(double obj, CancellationToken token);

        /// <summary>
        /// write float asynchronously
        /// </summary>
        /// <param name="obj">float to be written</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>the handler to the task</returns>
        Task WriteFloatAsync(float obj, CancellationToken token);

        /// <summary>
        /// write long asynchronously
        /// </summary>
        /// <param name="obj">long to be written</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>the handler to the task</returns>
        Task WriteLongAsync(long obj, CancellationToken token);

        /// <summary>
        /// write bool asynchronously
        /// </summary>
        /// <param name="obj">bool to be written</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>the handler to the task</returns>
        Task WriteBooleanAsync(bool obj, CancellationToken token);

        /// <summary>
        /// write integer asynchronously
        /// </summary>
        /// <param name="obj">integer to be written</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>the handler to the task</returns>
        Task WriteInt32Async(int obj, CancellationToken token);

        /// <summary>
        /// write short asynchronously
        /// </summary>
        /// <param name="obj">short to be written</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>the handler to the task</returns>
        Task WriteInt16Async(short obj, CancellationToken token);

        /// <summary>
        /// write string asynchronously
        /// </summary>
        /// <param name="obj">string to be written</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>the handler to the task</returns>
        Task WriteStringAsync(string obj, CancellationToken token);

        /// <summary>
        /// write bytes to the byte array asynchronously
        /// </summary>
        /// <param name="buffer">byte array from which to read</param>
        /// <param name="index">starting index in the array</param>
        /// <param name="count">number of bytes to write</param>
        /// <param name="token">Cancellation token</param>
        /// <returns>the handler to the task</returns>
        Task WriteAsync(byte[] buffer, int index, int count, CancellationToken token);
    }
}
