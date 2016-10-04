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

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Network.Group.Task.Impl;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Group.Operators.Impl
{
    /// <summary>
    /// Group Communication operator used to receive a sublist of messages sent
    /// from the IScatterSender.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    [Private]
    public sealed class ScatterReceiver<T> : IScatterReceiver<T>, IGroupCommOperatorInternal
    {
        private const int DefaultVersion = 1;
        private readonly IOperatorTopology<T> _topology;
        private readonly bool _initialize;

        /// <summary>
        /// Creates a new ScatterReceiver.
        /// </summary>
        /// <param name="operatorName">The name of the scatter operator</param>
        /// <param name="groupName">The name of the operator's CommunicationGroup</param>
        /// <param name="initialize">Require Topology Initialize to be called to wait for all task being registered. 
        /// Default is true. For unit testing, it can be set to false.</param>
        /// <param name="topology">The task's operator topology graph</param>
        [Inject]
        private ScatterReceiver(
            [Parameter(typeof(GroupCommConfigurationOptions.OperatorName))] string operatorName,
            [Parameter(typeof(GroupCommConfigurationOptions.CommunicationGroupName))] string groupName,
            [Parameter(typeof(GroupCommConfigurationOptions.Initialize))] bool initialize,
            OperatorTopology<T> topology)
        {
            OperatorName = operatorName;
            GroupName = groupName;
            Version = DefaultVersion;
            _topology = topology;
            _initialize = initialize;
        }

        /// <summary>
        /// Returns the name of the reduce operator
        /// </summary>
        public string OperatorName { get; }

        /// <summary>
        /// Returns the name of the operator's CommunicationGroup.
        /// </summary>
        public string GroupName { get; }

        /// <summary>
        /// Returns the operator version.
        /// </summary>
        public int Version { get; }

        /// <summary>
        /// Returns the class used to reduce incoming messages sent by ReduceSenders.
        /// </summary>
        public IReduceFunction<T> ReduceFunction { get; private set; }

        /// <summary>
        /// Receive a sublist of messages sent from the IScatterSender.
        /// </summary>
        /// <returns>The sublist of messages</returns>
        public List<T> Receive(CancellationTokenSource cancellationSource = null)
        {
            IList<T> elements = _topology.ReceiveListFromParent(cancellationSource);
            _topology.ScatterToChildren(elements, MessageType.Data);
            return elements.ToList();
        }

        /// <summary>
        /// Ensure all parent and children nodes in the topology are registered with the Name Service.
        /// </summary>
        void IGroupCommOperatorInternal.WaitForRegistration(CancellationTokenSource cancellationSource)
        {
            if (_initialize)
            {
                _topology.Initialize(cancellationSource);
            }
        }
    }
}
