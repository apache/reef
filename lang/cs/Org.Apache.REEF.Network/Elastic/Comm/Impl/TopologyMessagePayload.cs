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
    /// Messages sent by the driver to operators. 
    /// This message contains information for the destination node on the topology.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal class TopologyMessagePayload : DriverMessagePayload
    {
        /// <summary>
        /// Create a driver message payload containing topology updates.
        /// </summary>
        /// <param name="updates">The topology updates</param>
        /// <param name="toRemove">Whether the updates are additions to the current topology state or nodes removal</param>
        /// <param name="subscriptionName">The subscription context for the message</param>
        /// <param name="operatorId">The id of the operator receiving the topology update</param>
        /// <param name="iteration">The iteration in which the update takes effect</param>
        public TopologyMessagePayload(DriverMessagePayloadType type, List<TopologyUpdate> updates, string subscriptionName, int operatorId, int iteration)
            : base(subscriptionName, operatorId, iteration)
        {
            PayloadType = type;
            TopologyUpdates = updates;
        }

        /// <summary>
        /// Clone the message.
        /// </summary>
        /// <returns>An object containing the shallow copy of the message.</returns>
        public override object Clone()
        {
            var updatesClone = new List<TopologyUpdate>();

            foreach (var update in TopologyUpdates)
            {
                var clone = new TopologyUpdate(update.Node, update.Children, update.Root);
                updatesClone.Add(update);
            }

            return TopologyMessageBuilder(PayloadType, updatesClone, SubscriptionName, OperatorId, Iteration);
        }

        /// <summary>
        /// The updates for the topology.
        /// </summary>
        internal List<TopologyUpdate> TopologyUpdates { get; private set; }

        /// <summary>
        /// Creates a topology message payload out of memory buffer. 
        /// </summary>
        /// <param name="data">The buffer containing a serialized message payload</param>
        /// <param name="offset">The offset where to start the deserialization process</param>
        /// <returns>A topology message payload</returns>
        internal static DriverMessagePayload From(DriverMessagePayloadType type, byte[] data, int offset = 0)
        {
            int length = BitConverter.ToInt32(data, offset);
            offset += sizeof(int);
            List<TopologyUpdate> updates = TopologyUpdate.Deserialize(data, length, offset);
            offset += length;

            length = BitConverter.ToInt32(data, offset);
            offset += sizeof(int);
            string subscription = ByteUtilities.ByteArraysToString(data, offset, length);
            offset += length;
            int operatorId = BitConverter.ToInt32(data, offset);
            offset += sizeof(int);
            int iteration = BitConverter.ToInt32(data, offset);

            return TopologyMessageBuilder(type, updates, subscription, operatorId, iteration);
        }

        /// <summary>
        /// Utility method to serialize the payload for communication.
        /// </summary>
        /// <returns>The serialized payload</returns>
        internal override byte[] Serialize()
        {
            byte[] subscriptionBytes = ByteUtilities.StringToByteArrays(SubscriptionName);
            int offset = 0;
            var totalLengthUpdates = TopologyUpdates.Sum(x => x.Size);
            byte[] buffer = new byte[sizeof(int) + totalLengthUpdates + sizeof(int) + subscriptionBytes.Length + sizeof(bool) + sizeof(int) + sizeof(int)];

            Buffer.BlockCopy(BitConverter.GetBytes(totalLengthUpdates), 0, buffer, offset, sizeof(int));
            offset += sizeof(int);

            TopologyUpdate.Serialize(buffer, ref offset, TopologyUpdates);

            Buffer.BlockCopy(BitConverter.GetBytes(subscriptionBytes.Length), 0, buffer, offset, sizeof(int));
            offset += sizeof(int);
            Buffer.BlockCopy(subscriptionBytes, 0, buffer, offset, subscriptionBytes.Length);
            offset += subscriptionBytes.Length;
            Buffer.BlockCopy(BitConverter.GetBytes(OperatorId), 0, buffer, offset, sizeof(int));
            offset += sizeof(int);
            Buffer.BlockCopy(BitConverter.GetBytes(Iteration), 0, buffer, offset, sizeof(int));

            return buffer;
        }

        private static DriverMessagePayload TopologyMessageBuilder(DriverMessagePayloadType type, List<TopologyUpdate> updates, string subscriptionName, int operatorId, int iteration)
        {
            switch (type)
            {
                case DriverMessagePayloadType.Update:
                    return new UpdateMessagePayload(updates, subscriptionName, operatorId, iteration);
                case DriverMessagePayloadType.Failure:
                    return new FailureMessagePayload(updates, subscriptionName, operatorId, iteration);
                default:
                    throw new IllegalStateException($"Topology message type {type} not found.");
            }
        }
    }
}