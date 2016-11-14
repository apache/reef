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

using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.IO.Checkpoint;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Examples.Checkpointing
{
    /// <summary>
    /// A task that simply writes a checkpoint.
    /// </summary>
    public sealed class CheckpointTask : ITask
    {

        private static readonly Logger Logger = Logger.GetLogger(typeof(CheckpointTask));

        private readonly ICheckpointService _checkpointService;

        /// <summary>
        /// Task constructor: invoked by Tang.
        /// </summary>
        /// <param name="service"></param>
        [Inject]
        private CheckpointTask(ICheckpointService service)
        {
            _checkpointService = service;
        }

        /// <summary>
        /// Main method of the task
        /// </summary>
        /// <param name="memento">Info passed from the Driver; not used in this task.</param> 
        /// <returns></returns>
        public byte[] Call(byte[] memento)
        {
            ICheckpointWriter checkpoint = _checkpointService.Create();

            // save 0xdeadbeef in a checkpoint
            // this pattern is easily recognizable in a hex editor during debugging
            byte[] dummy = { 0xde, 0xad, 0xbe, 0xef };
            checkpoint.Write(ref dummy);
            ICheckpointID id = _checkpointService.Commit(checkpoint);
            
            Logger.Log(Level.Info, "Checkpoint with the id {0} commited.", id);

            return null;
        }

        public void Dispose()
        {
        }

    }
}