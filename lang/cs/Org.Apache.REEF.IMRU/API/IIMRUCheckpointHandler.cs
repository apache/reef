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

using Org.Apache.REEF.IMRU.OnREEF.CheckpointHandler;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.IMRU.API
{
    /// <summary>
    /// It is responsible for save and restore ITaskState object with the given ICodec
    /// </summary>
    [DefaultImplementation(typeof(IMRUCheckpointHandler))]
    public interface IIMRUCheckpointHandler
    {
        /// <summary>
        /// Persistent ITaskState object with the given ICodec.
        /// </summary>
        /// <param name="taskState"></param>
        /// <param name="codec"></param>
        void Persistent(ITaskState taskState, ICodec<ITaskState> codec);

        /// <summary>
        /// Restore the data and decode it with the given ICodec.
        /// </summary>
        /// <param name="codec"></param>
        /// <returns></returns>
        ITaskState Restore(ICodec<ITaskState> codec);

        /// <summary>
        /// Set a mark to indicate the result has been handled.
        /// </summary>
        void SetResult();

        /// <summary>
        /// Check if the result has been handled.
        /// </summary>
        bool GetResult();

        /// <summary>
        /// Reset persistent state
        /// </summary>
        void Reset();
    }
}