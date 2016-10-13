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
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Network.Group.Task.Impl;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Group.Operators.Impl
{
    /// <summary>
    /// Group Communication operator used to receive and reduce messages in pipelined fashion.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    [Private]
    public sealed class ReduceReceiver<T> : IReduceReceiver<T>, IGroupCommOperatorInternal
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ReduceReceiver<T>));
        private const int PipelineVersion = 2;
        private readonly IOperatorTopology<PipelineMessage<T>> _topology;
        private readonly PipelinedReduceFunction<T> _pipelinedReduceFunc;
        private readonly bool _initialize;

        /// <summary>
        /// Creates a new ReduceReceiver.
        /// </summary>
        /// <param name="operatorName">The name of the reduce operator</param>
        /// <param name="groupName">The name of the operator's CommunicationGroup</param>
        /// <param name="initialize">Require Topology Initialize to be called to wait for all task being registered. 
        /// Default is true. For unit testing, it can be set to false.</param>
        /// <param name="topology">The task's operator topology graph</param>
        /// <param name="reduceFunction">The class used to aggregate all incoming messages</param>
        /// <param name="dataConverter">The converter used to convert original
        /// message to pipelined ones and vice versa.</param>
        [Inject]
        private ReduceReceiver(
            [Parameter(typeof(GroupCommConfigurationOptions.OperatorName))] string operatorName,
            [Parameter(typeof(GroupCommConfigurationOptions.CommunicationGroupName))] string groupName,
            [Parameter(typeof(GroupCommConfigurationOptions.Initialize))] bool initialize,
            OperatorTopology<PipelineMessage<T>> topology,
            IReduceFunction<T> reduceFunction,
            IPipelineDataConverter<T> dataConverter)
        {
            OperatorName = operatorName;
            GroupName = groupName;
            Version = PipelineVersion;
            ReduceFunction = reduceFunction;
            PipelineDataConverter = dataConverter;
            _initialize = initialize;

            _pipelinedReduceFunc = new PipelinedReduceFunction<T>(ReduceFunction);
            _topology = topology;
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
        public IReduceFunction<T> ReduceFunction { get; }

        /// <summary>
        /// Returns the IPipelineDataConvert used to convert messages to pipeline form and vice-versa
        /// </summary>
        public IPipelineDataConverter<T> PipelineDataConverter { get; }

        /// <summary>
        /// Receives messages sent by all ReduceSenders and aggregates them
        /// using the specified IReduceFunction.
        /// </summary>
        /// <param name="cancellationSource">The cancellation token for the data reading operation cancellation</param>
        /// <returns>The single aggregated data</returns>
        public T Reduce(CancellationTokenSource cancellationSource = null)
        {
            PipelineMessage<T> message;
            var messageList = new List<PipelineMessage<T>>();

            do
            {
                message = _topology.ReceiveFromChildren(_pipelinedReduceFunc, cancellationSource);
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
