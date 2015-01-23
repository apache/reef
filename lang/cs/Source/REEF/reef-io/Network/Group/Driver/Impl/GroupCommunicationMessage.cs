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

namespace Org.Apache.Reef.IO.Network.Group.Driver.Impl
{
    /// <summary>
    /// Messages sent by MPI Operators
    /// </summary>
    public class GroupCommunicationMessage
    {
        /// <summary>
        /// Create new CommunicationGroupMessage.
        /// </summary>
        /// <param name="groupName">The name of the communication group</param>
        /// <param name="operatorName">The name of the MPI operator</param>
        /// <param name="source">The message source</param>
        /// <param name="destination">The message destination</param>
        /// <param name="data">The actual byte array of data</param>
        /// <param name="messageType">The type of message to send</param>
        public GroupCommunicationMessage(
            string groupName,
            string operatorName,
            string source,
            string destination,
            byte[] data,
            MessageType messageType)
        {
            GroupName = groupName;
            OperatorName = operatorName;
            Source = source;
            Destination = destination;
            Data = new[] { data };
            MsgType = messageType;
        }

        /// <summary>
        /// Create new CommunicationGroupMessage.
        /// </summary>
        /// <param name="groupName">The name of the communication group</param>
        /// <param name="operatorName">The name of the MPI operator</param>
        /// <param name="source">The message source</param>
        /// <param name="destination">The message destination</param>
        /// <param name="data">The actual byte array of data</param>
        /// <param name="messageType">The type of message to send</param>
        public GroupCommunicationMessage(
            string groupName,
            string operatorName,
            string source,
            string destination,
            byte[][] data,
            MessageType messageType)
        {
            GroupName = groupName;
            OperatorName = operatorName;
            Source = source;
            Destination = destination;
            Data = data;
            MsgType = messageType;
        }

        /// <summary>
        /// Returns the Communication Group name.
        /// </summary>
        public string GroupName { get; private set; }

        /// <summary>
        /// Returns the MPI Operator name.
        /// </summary>
        public string OperatorName { get; private set; }

        /// <summary>
        /// Returns the source of the message.
        /// </summary>
        public string Source { get; private set; }

        /// <summary>
        /// Returns the destination of the message.
        /// </summary>
        public string Destination { get; private set; }

        /// <summary>
        /// Returns the message data.
        /// </summary>
        public byte[][] Data { get; private set; }

        /// <summary>
        /// Returns the type of message being sent.
        /// </summary>
        public MessageType MsgType { get; private set; }
    }
}