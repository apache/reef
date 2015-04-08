/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Network.Group.Pipelining.Impl;

namespace Org.Apache.REEF.Network.Group.Operators.Impl
{
    /// <summary>
    /// The specification used to define Broadcast Operators.
    /// </summary>
    public class BroadcastOperatorSpec<T1, T2> : IOperatorSpec<T1, T2> where T2 : ICodec<T1>
    {
        /// <summary>
        /// Create a new BroadcastOperatorSpec.
        /// </summary>
        /// <param name="senderId">The identifier of the root sending Task.</param>
        /// <param name="codecType">The codec used to serialize messages.</param>
        public BroadcastOperatorSpec(string senderId)
        {
            SenderId = senderId;
             Codec = typeof(T2);
            PipelineDataConverter = new DefaultPipelineDataConverter<T1>();
        }

        /// <summary>
        /// Create a new BroadcastOperatorSpec.
        /// </summary>
        /// <param name="senderId">The identifier of the root sending Task.</param>
        /// <param name="dataConverter">The converter used to convert original
        /// message to pipelined ones and vice versa.</param>
        public BroadcastOperatorSpec(
            string senderId,
            IPipelineDataConverter<T1> dataConverter)
        {
            SenderId = senderId;
            Codec = typeof(T2);;
            PipelineDataConverter = dataConverter ?? new DefaultPipelineDataConverter<T1>();
        }

        /// <summary>
        /// Returns the IPipelineDataConverter class type used to convert messages to pipeline form and vice-versa
        /// </summary>
        public IPipelineDataConverter<T1> PipelineDataConverter { get; private set; }

        /// <summary>
        /// Returns the identifier of the Task that will broadcast data to other Tasks.
        /// </summary>
        public string SenderId { get; private set; }

        /// <summary>
        /// Returns the ICodec used to serialize messages.
        /// </summary>
        public Type Codec { get; private set; }
    }
}
