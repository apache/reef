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
    /// MPI operator used to receive and reduce messages.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public class ReduceReceiver<T> : IReduceReceiver<T>
    {
        private const int DefaultVersion = 1;

        private ICommunicationGroupNetworkObserver _networkHandler;
        private OperatorTopology<T> _topology;

        /// <summary>
        /// Creates a new ReduceReceiver.
        /// </summary>
        /// <param name="operatorName">The name of the reduce operator</param>
        /// <param name="groupName">The name of the operator's CommunicationGroup</param>
        /// <param name="topology">The task's operator topology graph</param>
        /// <param name="networkHandler">Handles incoming messages from other tasks</param>
        /// <param name="reduceFunction">The class used to aggregate all incoming messages</param>
        [Inject]
        public ReduceReceiver(
            [Parameter(typeof(MpiConfigurationOptions.OperatorName))] string operatorName,
            [Parameter(typeof(MpiConfigurationOptions.CommunicationGroupName))] string groupName,
            OperatorTopology<T> topology, 
            ICommunicationGroupNetworkObserver networkHandler,
            IReduceFunction<T> reduceFunction)
        {
            OperatorName = operatorName;
            GroupName = groupName;
            Version = DefaultVersion;
            ReduceFunction = reduceFunction;

            _networkHandler = networkHandler;
            _topology = topology;
            _topology.Initialize();

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

        /// <summary>
        /// Returns the class used to reduce incoming messages sent by ReduceSenders.
        /// </summary>
        public IReduceFunction<T> ReduceFunction { get; private set; }

        /// <summary>
        /// Receives messages sent by all ReduceSenders and aggregates them
        /// using the specified IReduceFunction.
        /// </summary>
        /// <returns>The single aggregated data</returns>
        public T Reduce()
        {
            return _topology.ReceiveFromChildren(ReduceFunction);
        }
    }
}
