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
using Org.Apache.REEF.Network.Group.Pipelining;

namespace Org.Apache.REEF.Network.Group.Operators
{
    /// <summary>
    /// The class used to aggregate pipelined messages sent by ReduceSenders.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    public class PipelinedReduceFunction<T> : IReduceFunction<PipelineMessage<T>>
    {
        public PipelinedReduceFunction(IReduceFunction<T> baseReduceFunc)
        {
            BaseReduceFunc = baseReduceFunc;
        }
        
        /// <summary>
        /// Reduce the IEnumerable of pipeline messages into one pipeline message.
        /// </summary>
        /// <param name="elements">The pipeline messages to reduce</param>
        /// <returns>The reduced pipeline message</returns>
        public PipelineMessage<T> Reduce(IEnumerable<PipelineMessage<T>> elements)
        {
            List<T> messageList = new List<T>();
            bool isLast = false;

            foreach (var message in elements)
            {
                messageList.Add(message.Data);
                isLast = message.IsLast;
            }

            return new PipelineMessage<T>(BaseReduceFunc.Reduce(messageList), isLast);
        }

        /// <summary>
        /// The base reduce function class that operates on actual message type T.
        /// </summary>
        /// <typeparam name="T">The message type.</typeparam>
        private IReduceFunction<T> BaseReduceFunc { get; set; }
    }
}
