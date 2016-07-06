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

namespace Org.Apache.REEF.Network.Group.Driver.Impl
{
    /// <summary>
    /// Messages sent by MPI Operators.
    /// </summary>
    internal sealed class GroupCommunicationMessage<T> : GeneralGroupCommunicationMessage
    {
        /// <summary>
        /// Create new CommunicationGroupMessage.
        /// </summary>
        /// <param name="groupName">The name of the communication group</param>
        /// <param name="operatorName">The name of the MPI operator</param>
        /// <param name="source">The message source</param>
        /// <param name="destination">The message destination</param>
        /// <param name="message">The actual Writable message</param>
        internal GroupCommunicationMessage(
            string groupName,
            string operatorName,
            string source,
            string destination,
            T message)
            : base(groupName, operatorName, source, destination, typeof(T))
        {
            Data = new[] { message };
        }

        /// <summary>
        /// Create new CommunicationGroupMessage.
        /// </summary>
        /// <param name="groupName">The name of the communication group</param>
        /// <param name="operatorName">The name of the MPI operator</param>
        /// <param name="source">The message source</param>
        /// <param name="destination">The message destination</param>
        /// <param name="message">The actual Writable message array</param>
        internal GroupCommunicationMessage(
            string groupName,
            string operatorName,
            string source,
            string destination,
            T[] message)
            : base(groupName, operatorName, source, destination, typeof(T))
        {
            Data = message;
        }

        /// <summary>
        /// Returns the array of messages.
        /// </summary>
        internal T[] Data
        {
            get;
            private set;
        }
    }
}