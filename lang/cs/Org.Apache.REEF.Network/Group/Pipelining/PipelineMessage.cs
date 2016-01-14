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

namespace Org.Apache.REEF.Network.Group.Pipelining
{
    /// <summary>
    /// the message for pipelined communication
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public sealed class PipelineMessage<T>
    {
        /// <summary>
        /// Create new PipelineMessage.
        /// </summary>
        /// <param name="data">The actual byte data</param>
        /// <param name="isLast">Whether this is last pipeline message</param>
        public PipelineMessage(T data, bool isLast)
        {
            Data = data;
            IsLast = isLast;
        }

        /// <summary>
        /// Returns the actual message
        /// </summary>
        public T Data { get; private set; }
        
        /// <summary>
        /// Returns whether this is the last pipelined message
        /// </summary>
        public bool IsLast { get; private set; }
    }
}
