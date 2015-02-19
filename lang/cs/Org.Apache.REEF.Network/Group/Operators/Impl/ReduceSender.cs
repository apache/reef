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
using System.Reactive;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Network.Group.Task.Impl;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Network.Group.Operators.Impl
{
    /// <summary>
    /// MPI Operator used to send messages to be reduced by the ReduceReceiver.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public class ReduceSender<T> : IReduceSender<T>
    {
        private const int DefaultVersion = 1;

        private readonly ICommunicationGroupNetworkObserver _networkHandler;
        private readonly OperatorTopology<T> _topology;
        private IReduceFunction<T> _reduceFunction;

        /// <summary>
        /// Creates a new ReduceSender.
        /// </summary>
        /// <param name="operatorName">The name of the reduce operator</param>
        /// <param name="groupName">The name of the reduce operator's CommunicationGroup</param>
        /// <param name="topology">The Task's operator topology graph</param>
        /// <param name="networkHandler">The handler used to handle incoming messages</param>
        [Inject]
        public ReduceSender(
            [Parameter(typeof(MpiConfigurationOptions.OperatorName))] string operatorName,
            [Parameter(typeof(MpiConfigurationOptions.CommunicationGroupName))] string groupName,
            OperatorTopology<T> topology, 
            ICommunicationGroupNetworkObserver networkHandler,
            IReduceFunction<T> reduceFunction)
        {
            OperatorName = operatorName;
            GroupName = groupName;
            Version = DefaultVersion;
            _reduceFunction = reduceFunction;
            _networkHandler = networkHandler;
            _topology = topology;
            _topology.Initialize();

            var msgHandler = Observer.Create<GroupCommunicationMessage>(message => _topology.OnNext(message));
            _networkHandler.Register(operatorName, msgHandler);
        }

        /// <summary>
        /// Returns the name of the reduce operator.
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
        /// Sends the data to the operator's ReduceReceiver to be aggregated.
        /// </summary>
        /// <param name="data">The data to send</param>
        public void Send(T data)
        {
            if (data == null)
            {
                throw new ArgumentNullException("data");    
            }

            var reducedValueOfChildren = _topology.ReceiveFromChildren(_reduceFunction);
            var mergeddData = new List<T>();
            mergeddData.Add(data);
            if (reducedValueOfChildren != null)
            {
                mergeddData.Add(reducedValueOfChildren);
            }
            T reducedValue = _reduceFunction.Reduce(mergeddData);

            _topology.SendToParent(reducedValue, MessageType.Data);
            //_topology.SendToParent(data, MessageType.Data);
        }
    }
}
