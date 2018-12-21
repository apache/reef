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

using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;
using System;

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    /// <summary>
    /// Checkpointable state wrapping an immutable object. 
    /// Since immutable when creating a checkpoint we don't need a copy.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [Unstable("0.16", "API may change")]
    public class CheckpointableImmutableObject<T> : ICheckpointableState
    {
        protected ICheckpointState _checkpoint;

        [Inject]
        private CheckpointableImmutableObject(
            [Parameter(typeof(OperatorParameters.Checkpointing))] int level,
            ICheckpointState checkpoint) : this()
        {
            Level = (CheckpointLevel)level;
            _checkpoint = checkpoint;
        }

        /// <summary>
        /// Basic constructor returning a checkponitable object with default state and iteration number = 0.
        /// </summary>
        protected CheckpointableImmutableObject()
        {
            Level = 0;
            State = default;
        }

        /// <summary>
        /// The current checkpoint level.
        /// </summary>
        public CheckpointLevel Level { get; internal set; }

        /// <summary>
        /// The actual state to checkpoint.
        /// </summary>
        internal T State { get; set; }

        /// <summary>
        /// Make the given input state a checkpointable state.
        /// </summary>
        /// <param name="state">The state that needs to be make checkpointable</param>
        public void MakeCheckpointable(object model)
        {
            State = (T)model;
        }

        /// <summary>
        /// Checkpoint the current state.
        /// </summary>
        /// <returns>A checkpoint state</returns>
        public virtual ICheckpointState Checkpoint()
        {
            switch (Level)
            {
                case CheckpointLevel.EphemeralMaster:
                case CheckpointLevel.EphemeralAll:
                    return _checkpoint.Create(State);
                default:
                    throw new ArgumentException($"Level {Level} not recognized.");
            }
        }

        /// <summary>
        /// Create a new empty checkpointable state from the current one.
        /// </summary>
        /// <param name="iteration">The current iteration for which we need to create a new checkpointable state</param>
        /// <returns>An empty checkpointable state</returns>
        public virtual ICheckpointableState Create()
        {
            return new CheckpointableImmutableObject<T>()
            {
                Level = Level,
            };
        }
    }
}
