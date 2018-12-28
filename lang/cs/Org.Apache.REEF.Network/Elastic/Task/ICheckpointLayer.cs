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

using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;
using System;

namespace Org.Apache.REEF.Network.Elastic.Task
{
    /// <summary>
    /// Checkpointing service interface used to locally checkpoint some task state or retrieve previously checkpointed local / remote states.
    /// </summary>
    [Unstable("0.16", "API may change")]
    [DefaultImplementation(typeof(CentralizedCheckpointLayer))]
    internal interface ICheckpointLayer : IDisposable
    {
        /// <summary>
        /// The service for communicating with the other available nodes.
        /// </summary>
        CommunicationLayer CommunicationLayer { set; }

        /// <summary>
        /// Register the current task id as well as notify the root task for the operator.
        /// </summary>
        /// <param name="stageName">The name of the stage</param>
        /// <param name="operatorId">The operator identifier</param>
        /// <param name="taskId">The identifier of the current task</param>
        /// <param name="rootTaskId">The identifier of the root task of the operator</param>
        void RegisterNode(string stageName, int operatorId, string taskId, string rootTaskId);

        /// <summary>
        /// Checkpoint the input state.
        /// </summary>
        /// <param name="state">The state to checkpoint</param>
        void Checkpoint(ICheckpointState state);

        /// <summary>
        /// Retrieve a checkpoint.
        /// </summary>
        /// <param name="checkpoint">The retrieve checkpoint if exists</param>
        /// <param name="taskId">The local task identifier</param>
        /// <param name="stageName">The name of the stage</param>
        /// <param name="operatorId">The operator identifier</param>
        /// <param name="iteration">The iteration number of the checkpoint</param>
        /// <param name="requestToRemote">Whether to request the checkpoint remotely if not found locally</param>
        /// <returns>True if the checkpoint is found, false otherwise</returns>
        bool GetCheckpoint(out ICheckpointState checkpoint, string taskId, string stageName, int operatorId, int iteration = -1, bool requestToRemote = true);

        /// <summary>
        /// Remove a checkpoint.
        /// </summary>
        /// <param name="stageName">The stage of the checkpoint to remove</param>
        /// <param name="operatorId">The operator id of the checkpoint to remove</param>
        void RemoveCheckpoint(string stageName, int operatorId);
    }
}