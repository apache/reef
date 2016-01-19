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

using System;
using System.Threading;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Network.Group.Driver.Impl
{
    /// <summary>
    /// Messages sent by MPI Operators. This is the class inherited by 
    /// GroupCommunicationMessage but seen by Network Service
    /// </summary>
    public class GeneralGroupCommunicationMessage
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
        protected GeneralGroupCommunicationMessage(
            string groupName,
            string operatorName,
            string source,
            string destination)
        {
            GroupName = groupName;
            OperatorName = operatorName;
            Source = source;
            Destination = destination;
        }

        /// <summary>
        /// Returns the Communication Group name.
        /// </summary>
        internal string GroupName { get; set; }

        /// <summary>
        /// Returns the MPI Operator name.
        /// </summary>
        internal string OperatorName { get; set; }

        /// <summary>
        /// Returns the source of the message.
        /// </summary>
        internal string Source { get; set; }

        /// <summary>
        /// Returns the destination of the message.
        /// </summary>
        internal string Destination { get; set; }
    }
}