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
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Group.Operators.Impl
{
    /// <summary>
    /// Group Communication operator used to scatter a list of elements to all
    /// of the IScatterReceivers.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    [Private]
    public sealed class ScatterSender<T> : IScatterSender<T>, IGroupCommOperatorInternal
    {
        private const int DefaultVersion = 1;
        private readonly IOperatorTopology<T> _topology;
        private readonly bool _initialize;

        /// <summary>
        /// Creates a new ScatterSender.
        /// </summary>
        /// <param name="operatorName">The name of the scatter operator</param>
        /// <param name="groupName">The name of the operator's Communication Group</param>
        /// <param name="initialize">Require Topology Initialize to be called to wait for all task being registered. 
        /// Default is true. For unit testing, it can be set to false.</param>
        /// <param name="topology">The operator topology</param>
        /// <param name="networkHandler">The network handler</param>
        [Inject]
        private ScatterSender(
            [Parameter(typeof(GroupCommConfigurationOptions.OperatorName))] string operatorName,
            [Parameter(typeof(GroupCommConfigurationOptions.CommunicationGroupName))] string groupName,
            [Parameter(typeof(GroupCommConfigurationOptions.Initialize))] bool initialize,
            OperatorTopology<T> topology,
            ICommunicationGroupNetworkObserver networkHandler)
        {
            OperatorName = operatorName;
            GroupName = groupName;
            Version = DefaultVersion;
            _topology = topology;
            _initialize = initialize;

            var msgHandler = Observer.Create<GeneralGroupCommunicationMessage>(message => topology.OnNext(message));
            networkHandler.Register(operatorName, msgHandler);
        }

        public string OperatorName { get; private set; }

        public string GroupName { get; private set; }

        public int Version { get; private set; }

        /// <summary>
        /// Split up the list of elements evenly and scatter each chunk
        /// to the IScatterReceivers.
        /// </summary>
        /// <param name="elements">The list of elements to send.</param>
        public void Send(List<T> elements)
        {
            _topology.ScatterToChildren(elements, MessageType.Data);
        }

        /// <summary>
        /// Split up the list of elements and scatter each chunk
        /// to the IScatterReceivers.  Each receiver will receive
        /// a sublist of the specified size.
        /// </summary>
        /// <param name="elements">The list of elements to send.</param>
        /// <param name="count">The size of each sublist</param>
        public void Send(List<T> elements, int count)
        {
            _topology.ScatterToChildren(elements, count, MessageType.Data);
        }

        /// <summary>
        /// Split up the list of elements and scatter each chunk
        /// to the IScatterReceivers in the specified task order.
        /// </summary>
        /// <param name="elements">The list of elements to send.</param>
        /// <param name="order">The list of task identifiers representing
        /// the order in which to scatter each sublist</param>
        public void Send(List<T> elements, List<string> order)
        {
            _topology.ScatterToChildren(elements, order, MessageType.Data);
        }

        /// <summary>
        /// Ensure all parent and children nodes in the topology are registered with teh Name Service.
        /// </summary>
        void IGroupCommOperatorInternal.WaitForRegistration()
        {
            if (_initialize)
            {
                _topology.Initialize();
            }
        }
    }
}
