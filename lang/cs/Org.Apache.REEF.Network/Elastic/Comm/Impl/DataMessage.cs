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

using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Comm.Impl
{
    /// <summary>
    /// Untyped data message sent by group communicationoOperators. This is the class inherited by 
    /// GroupCommunicationMessage but seen by the Network Service.
    /// DataMessages are untyped and used to semplify message propapagation through the
    /// communication layers that are type-agnostic.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal abstract class DataMessage : GroupCommunicationMessage
    {
        /// <summary>
        /// Constructor for an untyped data message.
        /// </summary>
        /// <param name="subscriptionName">The name of the subscription for the message</param>
        /// <param name="operatorId">The operator sending the message</param>
        /// <param name="iteration">The iteration in which the message is sent/valid</param>
        public DataMessage(string subscriptionName, int operatorId, int iteration)
            : base(subscriptionName, operatorId)
        {
            Iteration = iteration;
        }

        /// <summary>
        /// The iteration number for the message.
        /// </summary>
        internal int Iteration { get; set; }

        /// <summary>
        /// Clone the message.
        /// </summary>
        override public object Clone()
        {
            // The assumption is that messages are immutable therefore there is no need to clone them
            return this;
        }
    }

    /// <summary>
    /// A typed data message.
    /// </summary>
    /// <typeparam name="T">The type for the data message</typeparam>
    [Unstable("0.16", "API may change")]
    internal sealed class DataMessage<T> : DataMessage
    {
        /// <summary>
        /// Constructor of a typed data message.
        /// </summary>
        /// <param name="subscriptionName">The name of the subscription for the message</param>
        /// <param name="operatorId">The operator sending the message</param>
        /// <param name="iteration">The iteration in which the message is sent/valid</param>
        /// <param name="data">The data contained in the message</param>
        public DataMessage(
            string subscriptionName,
            int operatorId,
            int iteration, //// For the moment we consider iterations as ints. Maybe this would change in the future
            T data) : base(subscriptionName, operatorId, iteration)
        {
            Data = data;
        }

        /// <summary>
        /// The data contained in the message.
        /// </summary>
        internal T Data { get; set; }
    }
}