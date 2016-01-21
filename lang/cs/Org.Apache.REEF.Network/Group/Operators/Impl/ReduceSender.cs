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
using System.Reactive;
using System.Collections.Generic;
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
    /// Group Communication Operator used to send messages to be reduced by the ReduceReceiver in pipelined fashion.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    [Private]
    public sealed class ReduceSender<T> : IReduceSender<T>, IGroupCommOperatorInternal
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ReduceSender<T>));
        private const int PipelineVersion = 2;
        private readonly IOperatorTopology<PipelineMessage<T>> _topology;
        private readonly PipelinedReduceFunction<T> _pipelinedReduceFunc;
        private readonly bool _initialize;

        /// <summary>
        /// Creates a new ReduceSender.
        /// </summary>
        /// <param name="operatorName">The name of the reduce operator</param>
        /// <param name="groupName">The name of the reduce operator's CommunicationGroup</param>
        /// <param name="initialize">Require Topology Initialize to be called to wait for all task being registered. 
        /// Default is true. For unit testing, it can be set to false.</param>
        /// <param name="topology">The Task's operator topology graph</param>
        /// <param name="networkHandler">The handler used to handle incoming messages</param>
        /// <param name="reduceFunction">The function used to reduce the incoming messages</param>
        /// <param name="dataConverter">The converter used to convert original
        /// message to pipelined ones and vice versa.</param>
        [Inject]
        private ReduceSender(
            [Parameter(typeof(GroupCommConfigurationOptions.OperatorName))] string operatorName,
            [Parameter(typeof(GroupCommConfigurationOptions.CommunicationGroupName))] string groupName,
            [Parameter(typeof(GroupCommConfigurationOptions.Initialize))] bool initialize,
            OperatorTopology<PipelineMessage<T>> topology,
            ICommunicationGroupNetworkObserver networkHandler,
            IReduceFunction<T> reduceFunction,
            IPipelineDataConverter<T> dataConverter)
        {
            OperatorName = operatorName;
            GroupName = groupName;
            ReduceFunction = reduceFunction;

            Version = PipelineVersion;

            _pipelinedReduceFunc = new PipelinedReduceFunction<T>(ReduceFunction);
            _topology = topology;
            _initialize = initialize;

            var msgHandler = Observer.Create<GeneralGroupCommunicationMessage>(message => topology.OnNext(message));
            networkHandler.Register(operatorName, msgHandler);

            PipelineDataConverter = dataConverter;
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
        /// Get reduced data from children, reduce with the data given, then sends reduced data to parent
        /// </summary>
        public IReduceFunction<T> ReduceFunction { get; private set; }

        /// <summary>
        /// Returns the IPipelineDataConvert used to convert messages to pipeline form and vice-versa
        /// </summary>
        public IPipelineDataConverter<T> PipelineDataConverter { get; private set; }

        /// <summary>
        /// Sends the data to the operator's ReduceReceiver to be aggregated.
        /// </summary>
        /// <param name="data">The data to send</param>
        public void Send(T data)
        {
            var messageList = PipelineDataConverter.PipelineMessage(data);

            if (data == null)
            {
                throw new ArgumentNullException("data");
            }

            foreach (var message in messageList)
            {
                if (_topology.HasChildren())
                {
                    var reducedValueOfChildren = _topology.ReceiveFromChildren(_pipelinedReduceFunc);

                    var mergeddData = new List<PipelineMessage<T>> { message };

                    if (reducedValueOfChildren != null)
                    {
                        mergeddData.Add(reducedValueOfChildren);
                    }

                    var reducedValue = _pipelinedReduceFunc.Reduce(mergeddData);
                    _topology.SendToParent(reducedValue, MessageType.Data);
                }
                else
                {
                    _topology.SendToParent(message, MessageType.Data);
                }
            }
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
