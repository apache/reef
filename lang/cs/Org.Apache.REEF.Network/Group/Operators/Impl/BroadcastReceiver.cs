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
    /// Group Communication Operator used to receive broadcast messages in pipelined fashion.
    /// </summary>
    /// <typeparam name="T">The type of message being sent.</typeparam>
    [Private]
    public sealed class BroadcastReceiver<T> : IBroadcastReceiver<T>, IGroupCommOperatorInternal
    {
        private const int PipelineVersion = 2;
        private readonly IOperatorTopology<PipelineMessage<T>> _topology;
        private static readonly Logger Logger = Logger.GetLogger(typeof(BroadcastReceiver<T>));
        private readonly bool _initialize;

        /// <summary>
        /// Creates a new BroadcastReceiver.
        /// </summary>
        /// <param name="operatorName">The operator identifier</param>
        /// <param name="groupName">The name of the CommunicationGroup that the operator belongs to</param>
        /// <param name="initialize">Require Topology Initialize to be called to wait for all task being registered. 
        /// Default is true. For unit testing, it can be set to false.</param>
        /// <param name="topology">The node's topology graph</param>
        /// <param name="dataConverter">The converter used to convert original message to pipelined ones and vice versa.</param>
        [Inject]
        private BroadcastReceiver(
            [Parameter(typeof(GroupCommConfigurationOptions.OperatorName))] string operatorName,
            [Parameter(typeof(GroupCommConfigurationOptions.CommunicationGroupName))] string groupName,
            [Parameter(typeof(GroupCommConfigurationOptions.Initialize))] bool initialize,
            OperatorTopology<PipelineMessage<T>> topology,
            IPipelineDataConverter<T> dataConverter)
        {
            OperatorName = operatorName;
            GroupName = groupName;
            Version = PipelineVersion;
            PipelineDataConverter = dataConverter;
            _topology = topology;
            _initialize = initialize;
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
        /// Returns the IPipelineDataConvert used to convert messages to pipeline form and vice-versa
        /// </summary>
        public IPipelineDataConverter<T> PipelineDataConverter { get; private set; }

        /// <summary>
        /// Receive a message from parent BroadcastSender.
        /// </summary>
        /// <param name="cancellationSource">The cancellation token for the data reading operation cancellation</param>
        /// <returns>The incoming message</returns>
        public T Receive(CancellationTokenSource cancellationSource = null)
        {
            PipelineMessage<T> message;
            var messageList = new List<PipelineMessage<T>>();

            do
            {
                message = _topology.ReceiveFromParent(cancellationSource);

                if (_topology.HasChildren())
                {
                    _topology.SendToChildren(message, MessageType.Data);
                }

                messageList.Add(message);
            } 
            while (!message.IsLast);

            return PipelineDataConverter.FullMessage(messageList);
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
