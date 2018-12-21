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
using Org.Apache.REEF.Utilities.Attributes;
using System;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical
{
    /// <summary>
    /// Interface for topologies able to checkpoint state.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal interface ICheckpointingTopology : IDisposable
    {
        /// <summary>
        /// An internal (to the topology) checkpoint. This can be used to implement
        /// ephemeral level checkpoints.
        /// </summary>
        // For the moment the assumption is that only one object is stored
        ICheckpointState InternalCheckpoint { get; }

        /// <summary>
        /// Checkpoint the input state for the given iteration.
        /// </summary>
        /// <param name="state">The state to checkpoint</param>
        /// <param name="iteration">The iteration in which the checkpoint is happening</param>
        void Checkpoint(ICheckpointableState state, int iteration);

        /// <summary>
        /// Retrieve a previously saved checkpoint.
        /// The iteration number specificy which cehckpoint to retrieve, where -1
        /// is used by default to indicate the latest available checkpoint.
        /// </summary>
        /// <param name="checkpoint">The retrieved checkpoint</param>
        /// <param name="iteration">The iteration number for the checkpoint to retrieve.</param>
        /// <returns></returns>
        bool GetCheckpoint(out ICheckpointState checkpoint, int iteration = -1);
    }
}
