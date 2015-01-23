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
using Org.Apache.Reef.Tang.Annotations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.Reef.IO.Network.Group.Task
{
    /// <summary>
    /// Handles incoming messages sent to this Communication Group.
    /// </summary>
    [DefaultImplementation(typeof(CommunicationGroupNetworkObserver))]
    public interface ICommunicationGroupNetworkObserver : IObserver<GroupCommunicationMessage>
    {
        /// <summary>
        /// Registers the handler with the CommunicationGroupNetworkObserver.
        /// Messages that are to be sent to the operator specified by operatorName
        /// are handled by the given observer.
        /// </summary>
        /// <param name="operatorName">The name of the operator whose handler
        /// will be invoked</param>
        /// <param name="observer">The handler to invoke when messages are sent
        /// to the operator specified by operatorName</param>
        void Register(string operatorName, IObserver<GroupCommunicationMessage> observer);
    }
}
