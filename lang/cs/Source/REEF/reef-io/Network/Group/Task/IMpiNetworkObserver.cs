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

using Org.Apache.Reef.IO.Network.Group.Driver;
using Org.Apache.Reef.IO.Network.Group.Driver.Impl;
using Org.Apache.Reef.IO.Network.Group.Task.Impl;
using Org.Apache.Reef.IO.Network.NetworkService;
using Org.Apache.Reef.Tang.Annotations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.Reef.IO.Network.Group.Codec;

namespace Org.Apache.Reef.IO.Network.Group.Task
{
    /// <summary>
    /// Handles all incoming messages for this Task.
    /// </summary>
    [DefaultImplementation(typeof(MpiNetworkObserver))]
    public interface IMpiNetworkObserver : IObserver<NsMessage<GroupCommunicationMessage>>
    {
        /// <summary>
        /// Registers the network handler for the given CommunicationGroup.
        /// When messages are sent to the specified group name, the given handler
        /// will be invoked with that message.
        /// </summary>
        /// <param name="groupName">The group name for the network handler</param>
        /// <param name="commGroupHandler">The network handler to invoke when
        /// messages are sent to the given group.</param>
        void Register(string groupName, IObserver<GroupCommunicationMessage> commGroupHandler);
    }
}
