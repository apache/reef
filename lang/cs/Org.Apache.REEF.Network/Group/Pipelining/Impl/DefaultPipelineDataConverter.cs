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

using System.Collections.Generic;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Network.Group.Pipelining.Impl
{
    /// <summary>
    /// Default IPipelineDataConverter implementation
    /// This basically is a non-pipelined implementation that just packs the whole message in one single PipelineMessage
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public sealed class DefaultPipelineDataConverter<T> : IPipelineDataConverter<T>
    {
        [Inject]
        public DefaultPipelineDataConverter()
        {
        }

        /// <summary>
        /// Converts the original message to be communicated in to a single pipelined message
        /// </summary>
        /// <param name="message">The original message</param>
        /// <returns>The list of pipelined messages with only one element</returns>
        public List<PipelineMessage<T>> PipelineMessage(T message)
        {
            var messageList = new List<PipelineMessage<T>>();
            messageList.Add(new PipelineMessage<T>(message, true));
            return messageList;
        }

        /// <summary>
        /// Constructs the full final message from the communicated pipelined message
        /// </summary>
        /// <param name="pipelineMessage">The enumerator over received pipelined messages
        /// It is assumed to have only one element</param>
        /// <returns>The full constructed message</returns>
        public T FullMessage(List<PipelineMessage<T>> pipelineMessage)
        {
            if (pipelineMessage.Count != 1)
            {
                throw new System.Exception("Number of pipelined messages not equal to 1 in default IPipelineDataConverter implementation");
            }

            return pipelineMessage[0].Data;
        }
    }
}
