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

using System.Runtime.Serialization;
using Newtonsoft.Json;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.Examples.PipelinedBroadcastReduce
{
    /// <summary>
    /// Sample implementation of ITaskState that holds update task state 
    /// </summary>
    /// <typeparam name="TMapInput"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    [DataContract]
    internal sealed class UpdateTaskState<TMapInput, TResult> : ITaskState
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(UpdateTaskState<TMapInput, TResult>));

        [DataMember]
        internal TMapInput Input { get; set; }

        [DataMember]
        internal TResult Result { get; set; }

        /// <summary>
        /// Keep the current iteration number
        /// </summary>
        [DataMember]
        internal int Iterations { get; set; }

        /// <summary>
        /// Simple constructor for UpdateTaskState
        /// </summary>
        [Inject]
        [JsonConstructor]
        private UpdateTaskState()
        {
        }

        internal void Update(UpdateTaskState<TMapInput, TResult> taskState)
        {
            Input = taskState.Input;
            Result = taskState.Result;
            Iterations = taskState.Iterations;
        }
    }
}