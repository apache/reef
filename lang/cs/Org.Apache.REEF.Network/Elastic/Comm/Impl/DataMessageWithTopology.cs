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

using Org.Apache.REEF.Utilities.Attributes;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Comm.Impl
{
    /// <summary>
    /// In this message data and topology update information are sent together.
    /// This message is untyped and used to semplify message propapagation through the
    /// communication layers that are type-agnostic.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal abstract class DataMessageWithTopology : DataMessage
    {
        /// <summary>
        /// Constructor for the base untyped data message with topology.
        /// </summary>
        /// <param name="subscriptionName">The name of the subscription for the message</param>
        /// <param name="operatorId">The operator sending the message</param>
        /// <param name="iteration">The iteration in which the message is sent/valid</param>
        public DataMessageWithTopology(string subscriptionName, int operatorId, int iteration)
            : base(subscriptionName, operatorId, iteration)
        {
        }

        /// <summary>
        /// Some topology updates piggybacked to the main data message.
        /// </summary>
        internal List<TopologyUpdate> TopologyUpdates { get; set; }
    }

    /// <summary>
    /// Typed version for DataMessageWithTopology. This classis used at the communication entry-points.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [Unstable("0.16", "API may change")]
    internal class DataMessageWithTopology<T> : DataMessageWithTopology
    {
        /// <summary>
        /// Main constructor for data messages with topology information.
        /// </summary>
        /// <param name="subscriptionName">The name of the subscription for the message</param>
        /// <param name="operatorId">The operator sending the message</param>
        /// <param name="iteration">The iteration in which the message is sent/valid</param>
        /// <param name="data">The data contained in the message</param>
        /// <param name="updates">The topology updates being transmitted with the data</param>
        public DataMessageWithTopology(
            string subscriptionName,
            int operatorId,
            int iteration,
            T data,
            List<TopologyUpdate> updates) : base(subscriptionName, operatorId, iteration)
        {
            Data = data;
            TopologyUpdates = updates;
        }

        /// <summary>
        /// Constructor for a data message with topology but without topology updates.
        /// </summary>
        /// <param name="subscriptionName">The name of the subscription for the message</param>
        /// <param name="operatorId">The operator sending the message</param>
        /// <param name="iteration">The iteration in which the message is sent/valid</param>
        /// <param name="data">The data contained in the message</param>
        public DataMessageWithTopology(
            string subscriptionName,
            int operatorId,
            int iteration,
            T data) : this(subscriptionName, operatorId, iteration, data, new List<TopologyUpdate>())
        {
        }

        /// <summary>
        /// The data contained in the message.
        /// </summary>
        internal T Data { get; set; }
    }
}