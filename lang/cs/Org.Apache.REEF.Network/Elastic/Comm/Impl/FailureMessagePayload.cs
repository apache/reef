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
    /// Messages sent by the driver to operators. 
    /// This message contains information for the destination node on the topology.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal sealed class FailureMessagePayload : TopologyMessagePayload
    {
        /// <summary>
        /// Create a driver message payload containing topology updates 
        /// </summary>
        /// <param name="updates">The topology updates</param>
        /// <param name="toRemove">Whether the updates are additions to the current topology state or nodes removal</param>
        /// <param name="subscriptionName">The subscription context for the message</param>
        /// <param name="operatorId">The id of the operator receiving the topology update</param>
        /// <param name="iteration">The iteration in which the update takes effect</param>
        public FailureMessagePayload(List<TopologyUpdate> updates, string subscriptionName, int operatorId, int iteration)
            : base(DriverMessagePayloadType.Failure, updates, subscriptionName, operatorId, iteration)
        {
        }
    }
}