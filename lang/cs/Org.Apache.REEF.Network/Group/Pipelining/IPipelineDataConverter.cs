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

using System.Collections.Generic;
using Org.Apache.REEF.Network.Group.Pipelining.Impl;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Network.Group.Pipelining
{

    /// <summary>
    /// User specified class to convert the message to be communicated in to pipelining 
    /// amenable data and vice-versa 
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    // [DefaultImplementation(typeof(DefaultPipelineDataConverter<>))]
    public interface IPipelineDataConverter<T>
    {
        /// <summary>
        /// Converts the original message to be communicated in to a vector of pipelined messages.
        /// Each element of vector is communicated as a single logical unit.
        /// </summary>
        /// <param name="message">The original message</param>
        /// <returns>The list of pipelined messages</returns>
        List<PipelineMessage<T>> PipelineMessage(T message);

        /// <summary>
        /// Constructs the full final message from the vector of communicated pipelined messages
        /// </summary>
        /// <param name="pipelineMessage">The enumerator over received pipelined messages</param>
        /// <returns>The full constructed message</returns>
        T FullMessage(List<PipelineMessage<T>> pipelineMessage);
    }
}
