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

namespace Org.Apache.REEF.Network.Elastic.Task
{
    /// <summary>
    /// Interface defining the messages supported in tasks to driver communications.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal interface IDefaultTaskToDriverMessages
    {
        /// <summary>
        /// Notify the driver that operator <see cref="operatorId"/> is ready to join the
        /// group communication topology.
        /// </summary>
        /// <param name="taskId">The current task</param>
        /// <param name="operatorId">The identifier of the operator ready to join the topology</param>
        void JoinTopology(string taskId, string stageName, int operatorId);

        /// <summary>
        /// Send a notification to the driver for an update on topology state.
        /// </summary>
        /// <param name="taskId">The current task id</param>
        /// <param name="operatorId">The operator requiring the topology update</param>
        void TopologyUpdateRequest(string taskId, string stageName, int operatorId);

        /// <summary>
        /// Signal the driver that the current stage is completed.
        /// </summary>
        /// <param name="taskId">The current task identifier</param>
        void StageComplete(string taskId, string stageName);
    }
}
