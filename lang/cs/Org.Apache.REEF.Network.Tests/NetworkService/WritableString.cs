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
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Network.Tests.NetworkService
{
    /// <summary>
    /// Writable wrapper around the string class
    /// </summary>
    [Obsolete("Need to remove Iwritable and use IstreamingCodec. Please see Jira REEF-295 ", false)]
    public class WritableString : IWritable
    {
        /// <summary>
        /// Returns the actual string data
        /// </summary>
        public string Data { get; set; }
        
        /// <summary>
        /// Empty constructor for instantiation with reflection
        /// </summary>
        [Inject]
        public WritableString()
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="data">The string data</param>
        public WritableString(string data)
        {
            Data = data;
        }

        /// <summary>
        /// Reads the string
        /// </summary>
        /// <param name="reader">reader to read from</param>
        public void Read(IDataReader reader)
        {
            Data = reader.ReadString();
        }

        /// <summary>
        /// Writes the string
        /// </summary>
        /// <param name="writer">Writer to write</param>
        public void Write(IDataWriter writer)
        {
            writer.WriteString(Data);
        }

        /// <summary>
        /// Reads the string
        /// </summary>
        /// <param name="reader">reader to read from</param>
        /// <param name="token">the cancellation token</param>
        public async Task ReadAsync(IDataReader reader, CancellationToken token)
        {
            Data = await reader.ReadStringAsync(token);
        }

        /// <summary>
        /// Writes the string
        /// </summary>
        /// <param name="writer">Writer to write</param>
        /// <param name="token">the cancellation token</param>
        public async Task WriteAsync(IDataWriter writer, CancellationToken token)
        {
            await writer.WriteStringAsync(Data, token);
        }
    }
}
