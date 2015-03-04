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

using System.Reactive;
using System.Collections.Generic;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.Group.Task;
using Org.Apache.REEF.Network.Group.Task.Impl;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Group.Operators.Impl
{
    /// <summary>
    /// MPI operator used to receive and reduce messages in pipelined fashion.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public class ReduceReceiver<T> : IReduceReceiver<T>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ReduceReceiver<T>));

        private const int DefaultVersion = 1;

        private readonly ICommunicationGroupNetworkObserver _networkHandler;
        private readonly OperatorTopology<PipelineMessage<T>> _topology;
        private readonly PipelinedReduceFunction<T> _pipelinedReduceFunc;

        /// <summary>
        /// Creates a new ReduceReceiver.
        /// </summary>
        /// <param name="operatorName">The name of the reduce operator</param>
        /// <param name="groupName">The name of the operator's CommunicationGroup</param>
        /// <param name="topology">The task's operator topology graph</param>
        /// <param name="networkHandler">Handles incoming messages from other tasks</param>
        /// <param name="reduceFunction">The class used to aggregate all incoming messages</param>
        /// <param name="dataConverter">The converter used to convert original 
        /// message to pipelined ones and vice versa.</param>
        [Inject]
        public ReduceReceiver(
            [Parameter(typeof(MpiConfigurationOptions.OperatorName))] string operatorName,
            [Parameter(typeof(MpiConfigurationOptions.CommunicationGroupName))] string groupName,
            OperatorTopology<PipelineMessage<T>> topology,
            ICommunicationGroupNetworkObserver networkHandler,
            IReduceFunction<T> reduceFunction,
            IPipelineDataConverter<T> dataConverter)
        {
            OperatorName = operatorName;
            GroupName = groupName;
            Version = DefaultVersion;
            ReduceFunction = reduceFunction;

            _pipelinedReduceFunc = new PipelinedReduceFunction<T>(ReduceFunction);
            _networkHandler = networkHandler;
            _topology = topology;
            _topology.Initialize();

            var msgHandler = Observer.Create<GroupCommunicationMessage>(message => _topology.OnNext(message));
            _networkHandler.Register(operatorName, msgHandler);

            PipelineDataConverter = dataConverter;
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
        /// Returns the IPipelineDataConvert used to convert messages to pipeline form and vice-versa
        /// </summary>        
        public IPipelineDataConverter<T> PipelineDataConverter { get; private set; }

        /// <summary>
        /// Receives messages sent by all ReduceSenders and aggregates them
        /// using the specified IReduceFunction.
        /// </summary>
        /// <returns>The single aggregated data</returns>
        public T Reduce()
        {
            PipelineMessage<T> message;
            List<PipelineMessage<T>> messageList = new List<PipelineMessage<T>>();

            LOGGER.Log(Level.Info, "starting receiving reduce pipelined message");

            do
            {
                message = _topology.ReceiveFromChildren(_pipelinedReduceFunc);
                messageList.Add(message);

                /*var intmess = message as PipelineMessage<int[]>;

                if (intmess != null)
                {
                    LOGGER.Log(Level.Info, "***************** receiver receiving message ******************** " + intmess.IsLast);
                    for (int i = 0; i < intmess.Data.Length; i++)
                    {
                        LOGGER.Log(Level.Info, intmess.Data[i].ToString());
                    }
                }*/

            } while (!message.IsLast);
            //LOGGER.Log(Level.Info, "ending receiving reduce pipelined message");

            return PipelineDataConverter.FullMessage(messageList);
        }
    }
}
