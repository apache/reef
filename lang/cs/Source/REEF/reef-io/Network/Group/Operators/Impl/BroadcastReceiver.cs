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

using Org.Apache.Reef.IO.Network.Group.Config;
using Org.Apache.Reef.IO.Network.Group.Driver;
using Org.Apache.Reef.IO.Network.Group.Driver.Impl;
using Org.Apache.Reef.IO.Network.Group.Task;
using Org.Apache.Reef.IO.Network.Group.Task.Impl;
using Org.Apache.Reef.Tang.Annotations;
using System.Reactive;

namespace Org.Apache.Reef.IO.Network.Group.Operators.Impl
{
    /// <summary>
    /// MPI Operator used to receive broadcast messages.
    /// </summary>
    /// <typeparam name="T">The type of message being sent.</typeparam>
    public class BroadcastReceiver<T> : IBroadcastReceiver<T>
    {
        private const int DefaultVersion = 1;

        private ICommunicationGroupNetworkObserver _networkHandler;
        private OperatorTopology<T> _topology;

        /// <summary>
        /// Creates a new BroadcastReceiver.
        /// </summary>
        /// <param name="operatorName">The operator identifier</param>
        /// <param name="groupName">The name of the CommunicationGroup that the
        /// operator belongs to</param>
        /// <param name="topology">The node's topology graph</param>
        /// <param name="networkHandler">The incoming message handler</param>
        [Inject]
        public BroadcastReceiver(
            [Parameter(typeof(MpiConfigurationOptions.OperatorName))] string operatorName,
            [Parameter(typeof(MpiConfigurationOptions.CommunicationGroupName))] string groupName,
            OperatorTopology<T> topology, 
            ICommunicationGroupNetworkObserver networkHandler)
        {
            OperatorName = operatorName;
            GroupName = groupName;
            Version = DefaultVersion;

            _networkHandler = networkHandler;
            _topology = topology;
            _topology.Initialize();

            var msgHandler = Observer.Create<GroupCommunicationMessage>(message => _topology.OnNext(message));
            _networkHandler.Register(operatorName, msgHandler);
        }

        /// <summary>
        /// Returns the operator identifier.
        /// </summary>
        public string OperatorName { get; private set; }

        /// <summary>
        /// Returns the name of the CommunicationGroup that the operator belongs to.
        /// </summary>
        public string GroupName { get; private set; }

        /// <summary>
        /// Returns the operator version.
        /// </summary>
        public int Version { get; private set; }

        /// <summary>
        /// Receive a message from parent BroadcastSender.
        /// </summary>
        /// <returns>The incoming message</returns>
        public T Receive()
        {
            return _topology.ReceiveFromParent();
        }
    }
}
