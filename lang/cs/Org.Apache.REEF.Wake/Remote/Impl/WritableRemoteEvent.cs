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
using System.Diagnostics.Eventing.Reader;
using System.IO;
using System.Linq.Expressions;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Writable remote event class
    /// </summary>
    /// <typeparam name="T">Type of remote event message. It is assumed that T implements IWritable</typeparam>
    [Obsolete("Need to remove Iwritable and use IstreamingCodec. Please see Jira REEF-295 ", false)]
    public class WritableRemoteEvent<T> : IWritableRemoteEvent<T> where T : IWritable
    {
        /// <summary>
        /// Creates the Remote Event
        /// </summary>
        /// <param name="localEndPoint">Local Address</param>
        /// <param name="remoteEndPoint">Remote Address</param>
        /// <param name="source"></param>
        /// <param name="sink"></param>
        /// <param name="seq">The number of the message</param>
        /// <param name="value">Actual message</param>
        public WritableRemoteEvent(IPEndPoint localEndPoint, IPEndPoint remoteEndPoint, string source, string sink, long seq, T value)
        {
            LocalEndPoint = localEndPoint;
            RemoteEndPoint = remoteEndPoint;
            Source = source;
            Sink = sink;
            Value = value;
            Sequence = seq;
        }

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
        public WritableRemoteEvent()
        {
        }

        /// <summary>
        /// Local Address
        /// </summary>
        public IPEndPoint LocalEndPoint { get; set; }

        /// <summary>
        /// Remote Address
        /// </summary>
        public IPEndPoint RemoteEndPoint { get; set; }

        public string Source { get; set; }

        public string Sink { get; set; }

        /// <summary>
        /// The actual message
        /// </summary>
        public T Value { get; set; }

        public long Sequence { get; set; }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        public void Read(IDataReader reader)
        {
            Source = reader.ReadString();
            Sink = reader.ReadString();
            Sequence = reader.ReadLong();
            Value = Activator.CreateInstance<T>();
            Value.Read(reader);         
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="writer">The writer to which to write</param>
        public void Write(IDataWriter writer)
        {
            writer.WriteString(Source);
            writer.WriteString(Sink);
            writer.WriteLong(Sequence);
            Value.Write(writer);           
        }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        /// <param name="token">The cancellation token</param>
        public async Task ReadAsync(IDataReader reader, CancellationToken token)
        {
            Source = await reader.ReadStringAsync(token);
            Sink = await reader.ReadStringAsync(token);
            Sequence = await reader.ReadLongAsync(token);
            Value = Activator.CreateInstance<T>();
            await Value.ReadAsync(reader, token);      
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">The cancellation token</param>
        public async Task WriteAsync(IDataWriter writer, CancellationToken token)
        {
            await writer.WriteStringAsync(Source, token);
            await writer.WriteStringAsync(Sink, token);
            await writer.WriteLongAsync(Sequence, token);
            await Value.WriteAsync(writer, token);    
        }
    }
}
