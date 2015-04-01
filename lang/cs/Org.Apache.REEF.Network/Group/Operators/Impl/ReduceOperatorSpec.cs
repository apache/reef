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

namespace Org.Apache.REEF.Network.Group.Operators.Impl
{
    /// <summary>
    /// The specification used to define Reduce MPI Operators.
    /// </summary>
    public class ReduceOperatorSpec<T1, T2> : IOperatorSpec<T1, T2> where T2 : ICodec<T1>
    {
        /// <summary>
        /// Creates a new ReduceOperatorSpec.
        /// </summary>
        /// <param name="receiverId">The identifier of the task that
        /// will receive and reduce incoming messages.</param>
        /// <param name="codec">The codec used for serializing messages.</param>
        /// <param name="reduceFunction">The class used to aggregate all messages.</param>
        public ReduceOperatorSpec(
            string receiverId, 
            IReduceFunction<T1> reduceFunction)
        {
            ReceiverId = receiverId;
            Codec = typeof(T2);
            ReduceFunction = reduceFunction;
        }

        /// <summary>
        /// Returns the identifier for the task that receives and reduces
        /// incoming messages.
        /// </summary>
        public string ReceiverId { get; private set; }

        /// <summary>
        /// The codec used to serialize and deserialize messages.
        /// </summary>
        public Type Codec { get; private set; }

        /// <summary>
        /// The class used to aggregate incoming messages.
        /// </summary>
        public IReduceFunction<T1> ReduceFunction { get; private set; }
    }
}
