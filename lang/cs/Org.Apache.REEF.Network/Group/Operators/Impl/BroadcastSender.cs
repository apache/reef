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
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Network.Group.Task.Impl;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Group.Operators.Impl
{
    /// <summary>
    /// Group Communication Operator used to send messages to child Tasks in pipelined fashion.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    [Private]
    public sealed class BroadcastSender<T> : IBroadcastSender<T>, IGroupCommOperatorInternal
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(BroadcastSender<T>));
        private const int PipelineVersion = 2;
        private readonly IOperatorTopology<PipelineMessage<T>> _topology;
        private readonly bool _initialize;

        /// <summary>
        /// Creates a new BroadcastSender to send messages to other Tasks.
        /// </summary>
        /// <param name="operatorName">The identifier for the operator</param>
        /// <param name="groupName">The name of the CommunicationGroup that the operator
        /// belongs to</param>
        /// <param name="initialize">Require Topology Initialize to be called to wait for all task being registered. 
        /// Default is true. For unit testing, it can be set to false.</param>
        /// <param name="topology">The node's topology graph</param>
        /// <param name="dataConverter">The converter used to convert original
        /// message to pipelined ones and vice versa.</param>
        [Inject]
        private BroadcastSender(
            [Parameter(typeof(GroupCommConfigurationOptions.OperatorName))] string operatorName,
            [Parameter(typeof(GroupCommConfigurationOptions.CommunicationGroupName))] string groupName,
            [Parameter(typeof(GroupCommConfigurationOptions.Initialize))] bool initialize,
            OperatorTopology<PipelineMessage<T>> topology,
            IPipelineDataConverter<T> dataConverter)
        {
            _topology = topology;
            OperatorName = operatorName;
            GroupName = groupName;
            Version = PipelineVersion;
            PipelineDataConverter = dataConverter;
            _initialize = initialize;
        }

        /// <summary>
        /// Returns the identifier for the Group Communication operator.
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
        /// Returns the IPipelineDataConvert used to convert messages to pipeline form and vice-versa
        /// </summary>
        public IPipelineDataConverter<T> PipelineDataConverter { get; }

        /// <summary>
        /// Send the data to all BroadcastReceivers.
        /// </summary>
        /// <param name="data">The data to send.</param>
        public void Send(T data)
        {
            var messageList = PipelineDataConverter.PipelineMessage(data);

            if (data == null)
            {
                throw new ArgumentNullException("data");
            }

            foreach (var message in messageList)
            {
                _topology.SendToChildren(message, MessageType.Data);
            }
        }

        /// <summary>
        /// Ensure all parent and children nodes in the topology are registered with teh Name Service.
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
