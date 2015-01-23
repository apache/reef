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
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.Reef.IO.Network.Group.Config;
using Org.Apache.Reef.IO.Network.Group.Driver.Impl;
using Org.Apache.Reef.IO.Network.Group.Task;
using Org.Apache.Reef.IO.Network.Group.Task.Impl;
using Org.Apache.Reef.Tang.Annotations;

namespace Org.Apache.Reef.IO.Network.Group.Operators.Impl
{
    /// <summary>
    /// MPI operator used to scatter a list of elements to all
    /// of the IScatterReceivers.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public class ScatterSender<T> : IScatterSender<T>
    {
        private const int DefaultVersion = 1;

        private ICommunicationGroupNetworkObserver _networkHandler;
        private OperatorTopology<T> _topology;

        /// <summary>
        /// Creates a new ScatterSender.
        /// </summary>
        /// <param name="operatorName">The name of the scatter operator</param>
        /// <param name="groupName">The name of the operator's Communication Group</param>
        /// <param name="topology">The operator topology</param>
        /// <param name="networkHandler">The network handler</param>
        [Inject]
        public ScatterSender(
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
    }
}
