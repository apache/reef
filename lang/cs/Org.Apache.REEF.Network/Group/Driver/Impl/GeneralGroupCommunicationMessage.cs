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
using System.Threading;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Network.Group.Driver.Impl
{
    /// <summary>
    /// Messages sent by MPI Operators. This is the abstract class inherited by 
    /// WritableGroupCommunicationMessage but seen by Network Service
    /// </summary>
    // TODO: Need to remove Iwritable and use IstreamingCodec. Please see Jira REEF-295.
    public abstract class GeneralGroupCommunicationMessage : IWritable
    {        
        /// <summary>
        /// Empty constructor to allow instantiation by reflection
        /// </summary>
        protected GeneralGroupCommunicationMessage()
        {
        }

        /// <summary>
        /// Create new CommunicationGroupMessage.
        /// </summary>
        /// <param name="groupName">The name of the communication group</param>
        /// <param name="operatorName">The name of the MPI operator</param>
        /// <param name="source">The message source</param>
        /// <param name="destination">The message destination</param>
        /// <param name="messageType">The type of message to send</param>
        protected GeneralGroupCommunicationMessage(
            string groupName,
            string operatorName,
            string source,
            string destination,
            MessageType messageType)
        {
            GroupName = groupName;
            OperatorName = operatorName;
            Source = source;
            Destination = destination;
            MsgType = messageType;
        }

        /// <summary>
        /// Returns the Communication Group name.
        /// </summary>
        public string GroupName { get; internal set; }

        /// <summary>
        /// Returns the MPI Operator name.
        /// </summary>
        public string OperatorName { get; internal set; }

        /// <summary>
        /// Returns the source of the message.
        /// </summary>
        public string Source { get; internal set; }

        /// <summary>
        /// Returns the destination of the message.
        /// </summary>
        public string Destination { get; internal set; }

        /// <summary>
        /// Returns the type of message being sent.
        /// </summary>
        public MessageType MsgType { get; internal set; }

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        public abstract void Read(IDataReader reader);

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="writer">The writer to which to write</param>
        public abstract void Write(IDataWriter writer);

        /// <summary>
        /// Read the class fields.
        /// </summary>
        /// <param name="reader">The reader from which to read </param>
        /// <param name="token">The cancellation token</param>
        public abstract System.Threading.Tasks.Task ReadAsync(IDataReader reader, CancellationToken token);

        /// <summary>
        /// Writes the class fields.
        /// </summary>
        /// <param name="writer">The writer to which to write</param>
        /// <param name="token">The cancellation token</param>
        public abstract System.Threading.Tasks.Task WriteAsync(IDataWriter writer, CancellationToken token);
    }
}