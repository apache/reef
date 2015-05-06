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
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Hadoop.Avro;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Wake;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.NetworkService
{
    /// <summary>
    /// Writable Message sent between NetworkServices.</summary>
    /// <typeparam name="T">The type of data being sent. It is assumed to be Writable</typeparam>
    [Obsolete("Need to remove Iwritable and use IstreamingCodec. Please see Jira REEF-295 ", false)]
    public class WritableNsMessage<T> : IWritable where T : IWritable
    {
        private readonly IIdentifierFactory _factory;
        private IIdentifier _sourceId;
        private IIdentifier _destId;

        /// <summary>
        /// Constructor to allow instantiation by reflection
        /// </summary>
        public WritableNsMessage(IIdentifierFactory factory)
        {
            _factory = factory;
        }
        
        /// <summary>
        /// Create a new Writable NsMessage with no data.
        /// </summary>
        /// <param name="sourceId">The identifier of the sender</param>
        /// <param name="destId">The identifier of the receiver</param>
        public WritableNsMessage(IIdentifier sourceId, IIdentifier destId)
        {
            _sourceId = sourceId;
            _destId = destId;
            Data = new List<T>();
        }

        /// <summary>
        /// Create a new Writable NsMessage with data.
        /// </summary>
        /// <param name="sourceId">The identifier of the sender</param>
        /// <param name="destId">The identifier of the receiver</param>
        /// <param name="message">The message to send</param>
        public WritableNsMessage(IIdentifier sourceId, IIdentifier destId, T message)
        {
            _sourceId = sourceId;
            _destId = destId;
            Data = new List<T> {message};
        }

        /// <summary>
        /// A list of data being sent in the message.
        /// </summary>
        public List<T> Data { get; set; }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        public void Read(IDataReader reader)
        {
            _sourceId = _factory.Create(reader.ReadString());
            _destId = _factory.Create(reader.ReadString());
            int messageCount = reader.ReadInt32();

            Data = new List<T>();

            for (int index = 0; index < messageCount; index++)
            {
                Data.Add(Activator.CreateInstance<T>());

                if (Data[index] == null)
                {
                    throw new Exception("T type instance cannot be created from the stream data in Network Service Message");
                }

                Data[index].Read(reader);
            }
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="writer">The writer to which to write</param>
        public void Write(IDataWriter writer)
        {
            writer.WriteString(_sourceId.ToString());
            writer.WriteString(_destId.ToString());
            writer.WriteInt32(Data.Count);

            foreach (var data in Data)
            {
                data.Write(writer);
            }
        }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        /// <param name="token">The cancellation token</param>
        public async Task ReadAsync(IDataReader reader, CancellationToken token)
        {
            _sourceId = _factory.Create(await reader.ReadStringAsync(token));
            _destId = _factory.Create(await reader.ReadStringAsync(token));
            int messageCount = await reader.ReadInt32Async(token);

            Data = new List<T>();

            for (int index = 0; index < messageCount; index++)
            {
                Data.Add(Activator.CreateInstance<T>());

                if (Data[index] == null)
                {
                    throw new Exception("T type instance cannot be created from the stream data in Network Service Message");
                }

                await Data[index].ReadAsync(reader, token);
            }
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">The cancellation token</param>
        public async Task WriteAsync(IDataWriter writer, CancellationToken token)
        {
            await writer.WriteStringAsync(_sourceId.ToString(), token);
            await writer.WriteStringAsync(_destId.ToString(), token);
            await writer.WriteInt32Async(Data.Count, token);

            foreach (var data in Data)
            {
                data.Write(writer);
            }
        }
    }
}
