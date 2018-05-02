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

using Newtonsoft.Json;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce
{
    /// <summary>
    /// Codec for Update State
    /// </summary>
    internal sealed class UpdateTaskStateCodec : ICodec<ITaskState>
    {
        [Inject]
        private UpdateTaskStateCodec()
        {
        }

        /// <summary>
        /// Deserialize bytes into ITaskState object.
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public ITaskState Decode(byte[] data)
        {
            var str = ByteUtilities.ByteArraysToString(data);
            return JsonConvert.DeserializeObject<UpdateTaskState<int[], int[]>>(str);
        }

        /// <summary>
        /// Serialize ITaskState in to bytes.
        /// </summary>
        /// <param name="taskState"></param>
        /// <returns></returns>
        public byte[] Encode(ITaskState taskState)
        {
            var state = JsonConvert.SerializeObject(taskState);
            return ByteUtilities.StringToByteArrays(state);
        }
    }
}