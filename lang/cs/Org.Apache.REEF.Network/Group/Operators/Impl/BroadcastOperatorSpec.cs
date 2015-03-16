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

using System.Security.AccessControl;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Tang.Util;

namespace Org.Apache.REEF.Network.Group.Operators.Impl
{
    /// <summary>
    /// The specification used to define Broadcast Operators.
    /// </summary>
    public class BroadcastOperatorSpec<T> : IOperatorSpec<T>
    {
        /// <summary>
        /// Create a new BroadcastOperatorSpec.
        /// </summary>
        /// <param name="senderId">The identifier of the root sending Task.</param>
        /// <param name="codecType">The codec used to serialize messages.</param>
        public BroadcastOperatorSpec(string senderId, ICodec<T> codecType)
        {
            SenderId = senderId;
            Codec = codecType;
            PipelineDataConverter = null;
        }

        /// <summary>
        /// Create a new BroadcastOperatorSpec.
        /// </summary>
        /// <param name="senderId">The identifier of the root sending Task.</param>
        /// <param name="codecType">The codec used to serialize messages.</param>
        /// <param name="dataConverter">The converter used to convert original 
        /// message to pipelined ones and vice versa.</param>
        public BroadcastOperatorSpec(
            string senderId, 
            ICodec<T> codecType, 
            IPipelineDataConverter<T> dataConverter)
        {
            SenderId = senderId;
            Codec = codecType;
            PipelineDataConverter = dataConverter;
        }

        /// <summary>
        /// Returns the IPipelineDataConverter class type used to convert messages to pipeline form and vice-versa
        /// </summary>
        public IPipelineDataConverter<T> PipelineDataConverter { get; private set; }

        /// <summary>
        /// Returns the identifier of the Task that will broadcast data to other Tasks.
        /// </summary>
        public string SenderId { get; private set; }

        /// <summary>
        /// Returns the ICodec used to serialize messages.
        /// </summary>
        public ICodec<T> Codec { get; private set; }
    }
}
