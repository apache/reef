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
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Wake.Remote
{
    /// <summary>
    /// Represents a link between two endpoints
    /// </summary>
    public interface ILink<T> : IDisposable
    {
        /// <summary>
        /// Returns the local socket address
        /// </summary>
        IPEndPoint LocalEndpoint { get; }

        /// <summary>
        /// Returns the remote socket address
        /// </summary>
        IPEndPoint RemoteEndpoint { get; }

        /// <summary>
        /// Writes the value to this link asynchronously
        /// </summary>
        /// <param name="value">The data to write</param>
        /// <param name="token">The cancellation token</param>
        Task WriteAsync(T value, CancellationToken token);

        /// <summary>
        /// Writes the value to this link synchronously
        /// </summary>
        /// <param name="value">The data to write</param>
        void Write(T value);

        /// <summary>
        /// Reads the value from this link asynchronously
        /// </summary>
        /// <returns>The read data</returns>
        /// <param name="token">The cancellation token</param>
        Task<T> ReadAsync(CancellationToken token);

        /// <summary>
        /// Reads the value from this link synchronously
        /// </summary>
        /// <returns>The read data</returns>
        T Read();
    }
}
