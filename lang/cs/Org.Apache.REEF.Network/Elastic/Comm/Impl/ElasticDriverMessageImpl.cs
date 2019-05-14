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

using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Comm.Impl
{
    /// <summary>
    /// Message sent by the driver to operators on running tasks. 
    /// This message contains instructions from the driver to tasks's operators.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal sealed class ElasticDriverMessageImpl : IElasticDriverMessage
    {
        /// <summary>
        /// Create a new driver message.
        /// </summary>
        /// <param name="destinationTaskId">The message destination task</param>
        /// <param name="message">The message</param>
        public ElasticDriverMessageImpl(
            string destinationTaskId,
            DriverMessagePayload message)
        {
            Destination = destinationTaskId;
            Message = message;
        }

        /// <summary>
        /// The destination task of the message.
        public string Destination { get; }

        /// <summary>
        /// Operator and event specific payload of the message.
        /// </summary>
        public DriverMessagePayload Message { get; }

        /// <summary>
        /// Utility method to serialize the message for communication over the network.
        /// </summary>
        /// <returns>The serialized message</returns>
        public byte[] Serialize()
        {
            List<byte> buffer = new List<byte>();

            var destinationBytes = ByteUtilities.StringToByteArrays(Destination);
            buffer.AddRange(BitConverter.GetBytes(destinationBytes.Length));
            buffer.AddRange(destinationBytes);
            buffer.AddRange(BitConverter.GetBytes((short)Message.PayloadType));
            buffer.AddRange(Message.Serialize());

            return buffer.ToArray();
        }

        /// <summary>
        /// Creates a driver message payload out of the memory buffer. 
        /// </summary>
        /// <param name="data">The buffer containing a serialized message payload</param>
        /// <param name="offset">The offset where to start the deserialization process</param>
        /// <returns>A topology message payload</returns>
        public static ElasticDriverMessageImpl From(byte[] data, int offset = 0)
        {
            int destinationLength = BitConverter.ToInt32(data, offset);
            offset = 4;
            string destination = ByteUtilities.ByteArraysToString(data.Skip(offset).Take(destinationLength).ToArray());
            offset += destinationLength;

            DriverMessagePayloadType type = (DriverMessagePayloadType)BitConverter.ToUInt16(data, offset);
            offset += sizeof(ushort);

            DriverMessagePayload payload = null;

            switch (type)
            {
                case DriverMessagePayloadType.Update:
                case DriverMessagePayloadType.Failure:
                    payload = TopologyMessagePayload.From(type, data, offset);
                    break;
                default:
                    throw new IllegalStateException("Message type not recognized");
            }

            return new ElasticDriverMessageImpl(destination, payload);
        }
    }
}