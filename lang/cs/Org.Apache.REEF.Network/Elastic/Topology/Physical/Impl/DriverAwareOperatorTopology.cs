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

using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Utilities.Attributes;
using System;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical.Impl
{
    /// <summary>
    /// Abstract class for topologies able to receive messages from the driver.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal abstract class DriverAwareOperatorTopology : OperatorTopology, IObserver<DriverMessagePayload>
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="taskId">The identifier of the task the topology is running on</param>
        /// <param name="rootTaskId">The identifier of the root note in the topology</param>
        /// <param name="subscriptionName">The subscription name the topology is working on</param>
        /// <param name="operatorId">The identifier of the operator for this topology</param>
        public DriverAwareOperatorTopology(string taskId, string rootTaskId, string subscriptionName, int operatorId)
            : base(taskId, rootTaskId, subscriptionName, operatorId)
        {
        }

        /// <summary>
        /// Basic handler for messages coming from the driver.
        /// </summary>
        /// <param name="message">Message from the driver</param>
        public virtual void OnNext(DriverMessagePayload message)
        {
            switch (message.PayloadType)
            {
                case DriverMessagePayloadType.Ring:
                case DriverMessagePayloadType.Resume:
                case DriverMessagePayloadType.Update:
                case DriverMessagePayloadType.Failure:
                    break;
                default:
                    throw new ArgumentException($"Message type {message.PayloadType} not recognized.");
            }
        }

        #region Empty Handlers

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
        #endregion
    }
}
