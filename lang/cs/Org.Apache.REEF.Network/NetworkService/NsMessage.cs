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
using Org.Apache.REEF.Wake;

namespace Org.Apache.REEF.Network.NetworkService
{
    /// <summary>
    /// Message sent between NetworkServices
    /// </summary>
    /// <typeparam name="T">The type of data being sent</typeparam>
    public class NsMessage<T>
    {
        /// <summary>
        /// Create a new NsMessage with no data.
        /// </summary>
        /// <param name="sourceId">The identifier of the sender</param>
        /// <param name="destId">The identifier of the receiver</param>
        public NsMessage(IIdentifier sourceId, IIdentifier destId)
        {
            SourceId = sourceId;
            DestId = destId;
            Data = new List<T>();
        }

        /// <summary>
        /// Create a new NsMessage with data.
        /// </summary>
        /// <param name="sourceId">The identifier of the sender</param>
        /// <param name="destId">The identifier of the receiver</param>
        /// <param name="message">The message to send</param>
        public NsMessage(IIdentifier sourceId, IIdentifier destId, T message) 
        {
            SourceId = sourceId;
            DestId = destId;
            Data = new List<T> { message };
        }

        /// <summary>
        /// The identifier of the sender of the message.
        /// </summary>
        public IIdentifier SourceId { get; private set; }

        /// <summary>
        /// The identifier of the receiver of the message.
        /// </summary>
        public IIdentifier DestId { get; private set; }

        /// <summary>
        /// A list of data being sent in the message.
        /// </summary>
        public List<T> Data { get; private set; } 
    }
}
