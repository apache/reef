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
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.StreamingCodec;

namespace Org.Apache.REEF.Network.Group.Driver.Impl
{
    /// <summary>
    /// Messages sent by MPI Operators. This is the Writable version of GroupCommunicationMessage
    ///  class and will eventually replace it once everybody agrees with the design
    /// </summary>
    // TODO: Need to remove Iwritable and use IstreamingCodec. Please see Jira REEF-295.
    public sealed class GroupCommunicationMessage<T> : GeneralGroupCommunicationMessage
    {
        private readonly IStreamingCodec<T> _codec;

        /// <summary>
        /// Empty constructor to allow instantiation by reflection
        /// </summary>
        [Inject]
        private GroupCommunicationMessage(IStreamingCodec<T> codec)
        {
            _codec = codec;
        }

        /// <summary>
        /// Create new CommunicationGroupMessage.
        /// </summary>
        /// <param name="groupName">The name of the communication group</param>
        /// <param name="operatorName">The name of the MPI operator</param>
        /// <param name="source">The message source</param>
        /// <param name="destination">The message destination</param>
        /// <param name="message">The actual Writable message</param>
        /// <param name="messageType">The type of message to send</param>
        /// <param name="codec">Streaming Codec</param>
        public GroupCommunicationMessage(
            string groupName,
            string operatorName,
            string source,
            string destination,
            T message,
            MessageType messageType,
            IStreamingCodec<T> codec)
            : base(groupName, operatorName, source, destination, messageType)
        {
            _codec = codec;
            Data = new T[] { message };
        }

        /// <summary>
        /// Create new CommunicationGroupMessage.
        /// </summary>
        /// <param name="groupName">The name of the communication group</param>
        /// <param name="operatorName">The name of the MPI operator</param>
        /// <param name="source">The message source</param>
        /// <param name="destination">The message destination</param>
        /// <param name="message">The actual Writable message array</param>
        /// <param name="messageType">The type of message to send</param>
        /// <param name="codec">Streaming Codec</param>
        public GroupCommunicationMessage(
            string groupName,
            string operatorName,
            string source,
            string destination,
            T[] message,
            MessageType messageType,
            IStreamingCodec<T> codec)
            : base(groupName, operatorName, source, destination, messageType)
        {
            _codec = codec;
            Data = message;
        }

        /// <summary>
        /// Returns the array of messages.
        /// </summary>
        public T[] Data
        {
            get;
            set;
        }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        public override void Read(IDataReader reader)
        {
            GroupName = reader.ReadString();
            OperatorName = reader.ReadString();
            Source = reader.ReadString();
            Destination = reader.ReadString();

            int dataCount = reader.ReadInt32();

            if (dataCount == 0)
            {
                throw new Exception("Data Count in Group COmmunication Message cannot be zero");
            }

            MsgType = (MessageType)Enum.Parse(typeof(MessageType), reader.ReadString());
            Data = new T[dataCount];

            for (int index = 0; index < dataCount; index++)
            {
                Data[index] = _codec.Read(reader);

                if (Data[index] == null)
                {
                    throw new Exception("message instance cannot be created from the IDataReader in Group Communication Message");
                }
            }
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="writer">The writer to which to write</param>
        public override void Write(IDataWriter writer)
        {
            writer.WriteString(GroupName);
            writer.WriteString(OperatorName);
            writer.WriteString(Source);
            writer.WriteString(Destination);
            writer.WriteInt32(Data.Length);
            writer.WriteString(MsgType.ToString());

            foreach (var data in Data)
            {
                _codec.Write(data, writer);
            }
        }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        /// <param name="token">The cancellation token</param>
        public override async System.Threading.Tasks.Task ReadAsync(IDataReader reader, CancellationToken token)
        {
            GroupName = await reader.ReadStringAsync(token);
            OperatorName = await reader.ReadStringAsync(token);
            Source = await reader.ReadStringAsync(token);
            Destination = await reader.ReadStringAsync(token);

            int dataCount = await reader.ReadInt32Async(token);

            if (dataCount == 0)
            {
                throw new Exception("Data Count in Group COmmunication Message cannot be zero");
            }

            MsgType = (MessageType)Enum.Parse(typeof(MessageType), await reader.ReadStringAsync(token));
            Data = new T[dataCount];

            for (int index = 0; index < dataCount; index++)
            {
                Data[index] = await _codec.ReadAsync(reader, token);

                if (Data[index] == null)
                {
                    throw new Exception("message instance cannot be created from the IDataReader in Group Communication Message");
                }
            }
        }

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">The cancellation token</param>
        public override async System.Threading.Tasks.Task WriteAsync(IDataWriter writer, CancellationToken token)
        {
            await writer.WriteStringAsync(GroupName, token);
            await writer.WriteStringAsync(OperatorName, token);
            await writer.WriteStringAsync(Source, token);
            await writer.WriteStringAsync(Destination, token);
            await writer.WriteInt32Async(Data.Length, token);
            await writer.WriteStringAsync(MsgType.ToString(), token);

            foreach (var data in Data)
            {
                await _codec.WriteAsync(data, writer, token);
            }
        }
    }
}