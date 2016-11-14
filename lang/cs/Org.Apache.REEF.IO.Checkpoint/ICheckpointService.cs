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

using System.IO;

namespace Org.Apache.REEF.IO.Checkpoint
{
    public interface ICheckpointService
    {
        /// <summary>
        /// Creates a checkpoint and provides a System.IO.Stream to write to it.
        /// The name/location of the checkpoint are unknown to the user as of this time, in fact,
        /// the CheckpointID is not released to the user until commit is called.
        /// This makes enforcing atomicity of writes easy.
        /// </summary>
        ICheckpointWriter Create();

        /// <summary>
        /// Closes an existing checkpoint for writes and returns the CheckpointID that can be later
        /// used to get the read-only access to this checkpoint.
        ///
        /// Implementation  is supposed to return the CheckpointID to the caller only on the
        /// successful completion of checkpoint to guarantee atomicity of the checkpoint.
        /// </summary>
        ICheckpointID Commit(ICheckpointWriter stream);

        /// <summary>
        /// Aborts the current checkpoint. Garbage collection choices are
        /// left to the implementation.The CheckpointID is neither generated nor released to the
        /// client so the checkpoint is not accessible.
        /// </summary>
        void Abort(ICheckpointWriter stream);

        /// <summary>
        /// Discards an existing checkpoint identified by its CheckpointID.
        /// </summary>
        void Delete(ICheckpointID checkpointId);

        /// <summary>
        /// Returns a ICheckpointReader for reading a checkpoint identified by the CheckpointID.
        /// </summary>
        ICheckpointReader Open(ICheckpointID checkpointId);
    }
}