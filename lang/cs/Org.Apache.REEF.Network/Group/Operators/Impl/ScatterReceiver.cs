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

using System.Collections.Generic;
using System.Reactive;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Network.Group.Task.Impl;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Network.Group.Operators.Impl
{
    /// <summary>
    /// Group Communication operator used to receive a sublist of messages sent
    /// from the IScatterSender.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public class ScatterReceiver<T> : IScatterReceiver<T>
    {
        private const int DefaultVersion = 1;

        private readonly ICommunicationGroupNetworkObserver _networkHandler;
        private readonly OperatorTopology<T> _topology;
        private bool _isInitialize = false;

        /// <summary>
        /// Creates a new ScatterReceiver.
        /// </summary>
        /// <param name="operatorName">The name of the scatter operator</param>
        /// <param name="groupName">The name of the operator's CommunicationGroup</param>
        /// <param name="topology">The task's operator topology graph</param>
        /// <param name="networkHandler">Handles incoming messages from other tasks</param>
        [Inject]
        public ScatterReceiver(
            [Parameter(typeof(GroupCommConfigurationOptions.OperatorName))] string operatorName,
            [Parameter(typeof(GroupCommConfigurationOptions.CommunicationGroupName))] string groupName,
            OperatorTopology<T> topology, 
            ICommunicationGroupNetworkObserver networkHandler)
        {
            OperatorName = operatorName;
            GroupName = groupName;
            Version = DefaultVersion;

            _networkHandler = networkHandler;
            _topology = topology;

            var msgHandler = Observer.Create<GroupCommunicationMessage>(message => _topology.OnNext(message));
            _networkHandler.Register(operatorName, msgHandler);
        }

        /// <summary>
        /// Returns the name of the reduce operator
        /// </summary>
        public string OperatorName { get; private set; }

        /// <summary>
        /// Returns the name of the operator's CommunicationGroup.
        /// </summary>
        public string GroupName { get; private set; }

        /// <summary>
        /// Returns the operator version.
        /// </summary>
        public int Version { get; private set; }

        public void Initialize()
        {
            if (!_isInitialize)
            {
                _topology.Initialize();
                _isInitialize = true;
            }
        }

        /// <summary>
        /// Returns the class used to reduce incoming messages sent by ReduceSenders.
        /// </summary>
        public IReduceFunction<T> ReduceFunction { get; private set; }

        /// <summary>
        /// Receive a sublist of messages sent from the IScatterSender.
        /// </summary>
        /// <returns>The sublist of messages</returns>
        public List<T> Receive()
        {
            List<T> elements = _topology.ReceiveListFromParent();
            _topology.ScatterToChildren(elements, MessageType.Data);
            return elements;
        }
    }
}
