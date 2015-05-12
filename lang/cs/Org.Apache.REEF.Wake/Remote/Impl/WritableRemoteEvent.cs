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
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Writable remote event class
    /// </summary>
    /// <typeparam name="T">Type of remote event message. It is assumed that T implements IWritable</typeparam>
    [Obsolete("Need to remove Iwritable and use IstreamingCodec. Please see Jira REEF-295 ", false)]
    internal sealed class WritableRemoteEvent<T> : IWritableRemoteEvent<T> where T : IWritable
    {
        private readonly IInjector _injector;

        /// <summary>
        /// Creates the Remote Event
        /// </summary>
        /// <param name="localEndpoint">Local Address</param>
        /// <param name="remoteEndpoint">Remote Address</param>
        /// <param name="value">Actual message</param>
        public WritableRemoteEvent(IPEndPoint localEndpoint, IPEndPoint remoteEndpoint, T value)
        {
            LocalEndPoint = localEndpoint;
            RemoteEndPoint = remoteEndpoint;
            Value = value;
        }

        /// <summary>
        /// Creates empty Remote Event
        /// </summary>
        [Inject]
        public WritableRemoteEvent(IInjector injector)
        {
            _injector = injector;
        }

        /// <summary>
        /// Local Address
        /// </summary>
        public IPEndPoint LocalEndPoint { get; set; }

        /// <summary>
        /// Remote Address
        /// </summary>
        public IPEndPoint RemoteEndPoint { get; set; }

        /// <summary>
        /// The actual message
        /// </summary>
        public T Value { get; set; }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        public void Read(IDataReader reader)
        {
            Value = (T)_injector.ForkInjector().GetInstance(typeof(T));
            Value.Read(reader);         
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="writer">The writer to which to write</param>
        public void Write(IDataWriter writer)
        {
            Value.Write(writer);           
        }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        /// <param name="token">The cancellation token</param>
        public async Task ReadAsync(IDataReader reader, CancellationToken token)
        {
            Value = (T)_injector.ForkInjector().GetInstance(typeof(T));
            await Value.ReadAsync(reader, token);      
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">The cancellation token</param>
        public async Task WriteAsync(IDataWriter writer, CancellationToken token)
        {
            await Value.WriteAsync(writer, token);    
        }
    }
}
