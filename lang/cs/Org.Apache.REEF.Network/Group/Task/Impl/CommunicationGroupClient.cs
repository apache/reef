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
using System.Collections.Generic;
using System.Reflection;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Operators.Impl;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    /// <summary>
    ///  Used by Tasks to fetch Group Communication Operators in the group configured by the driver.
    /// </summary>
    internal sealed class CommunicationGroupClient : ICommunicationGroupClientInternal
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(CommunicationGroupClient));
        private readonly Dictionary<string, object> _operators;

        /// <summary>
        /// Creates a new CommunicationGroupClient.
        /// </summary>
        /// <param name="groupName">The name of the CommunicationGroup</param>
        /// <param name="operatorConfigs">The serialized operator configurations</param>
        /// <param name="groupCommNetworkObserver">The handler for all incoming messages
        /// across all Communication Groups</param>
        /// <param name="configSerializer">Used to deserialize operator configuration.</param>
        /// <param name="commGroupNetworkHandler">Communication group network observer that holds all the handlers for each operator.</param>
        /// <param name="injector">injector forked from the injector that creates this instance</param>
        [Inject]
        private CommunicationGroupClient(
            [Parameter(typeof(GroupCommConfigurationOptions.CommunicationGroupName))] string groupName,
            [Parameter(typeof(GroupCommConfigurationOptions.SerializedOperatorConfigs))] ISet<string> operatorConfigs,
            IGroupCommNetworkObserver groupCommNetworkObserver,
            AvroConfigurationSerializer configSerializer,
            ICommunicationGroupNetworkObserver commGroupNetworkHandler,
            IInjector injector)
        {
            _operators = new Dictionary<string, object>();

            GroupName = groupName;
            groupCommNetworkObserver.Register(groupName, commGroupNetworkHandler);

            foreach (string operatorConfigStr in operatorConfigs)
            {                
                IConfiguration operatorConfig = configSerializer.FromString(operatorConfigStr);

                IInjector operatorInjector = injector.ForkInjector(operatorConfig);
                string operatorName = operatorInjector.GetNamedInstance<GroupCommConfigurationOptions.OperatorName, string>(
                    GenericType<GroupCommConfigurationOptions.OperatorName>.Class);
                string msgType = operatorInjector.GetNamedInstance<GroupCommConfigurationOptions.MessageType, string>(
                    GenericType<GroupCommConfigurationOptions.MessageType>.Class);

                Type groupCommOperatorGenericInterface = typeof(IGroupCommOperator<>);
                Type groupCommOperatorInterface = groupCommOperatorGenericInterface.MakeGenericType(Type.GetType(msgType));
                var operatorObj = operatorInjector.GetInstance(groupCommOperatorInterface);
                _operators.Add(operatorName, operatorObj);
            }
        }

        /// <summary>
        /// Returns the Communication Group name
        /// </summary>
        public string GroupName { get; private set; }

        /// <summary>
        /// Gets the BroadcastSender with the given name and message type.
        /// </summary>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="operatorName">The name of the Broadcast operator</param>
        /// <returns>The BroadcastSender</returns>
        public IBroadcastSender<T> GetBroadcastSender<T>(string operatorName)
        {
            return GetOperatorInstance<BroadcastSender<T>>(operatorName);
        }

        /// <summary>
        /// Gets the BroadcastReceiver with the given name and message type.
        /// </summary>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="operatorName">The name of the Broadcast operator</param>
        /// <returns>The BroadcastReceiver</returns>
        public IBroadcastReceiver<T> GetBroadcastReceiver<T>(string operatorName)
        {
            return GetOperatorInstance<BroadcastReceiver<T>>(operatorName);
        }

        /// <summary>
        /// Gets the ReduceSender with the given name and message type.
        /// </summary>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="operatorName">The name of the Reduce operator</param>
        /// <returns>The ReduceSender</returns>
        public IReduceSender<T> GetReduceSender<T>(string operatorName)
        {
            return GetOperatorInstance<ReduceSender<T>>(operatorName);
        }

        /// <summary>
        /// Gets the ReduceReceiver with the given name and message type.
        /// </summary>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="operatorName">The name of the Reduce operator</param>
        /// <returns>The ReduceReceiver</returns>
        public IReduceReceiver<T> GetReduceReceiver<T>(string operatorName)
        {
            return GetOperatorInstance<ReduceReceiver<T>>(operatorName);
        }

        /// <summary>
        /// Gets the ScatterSender with the given name and message type.
        /// </summary>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="operatorName">The name of the Scatter operator</param>
        /// <returns>The ScatterSender</returns>
        public IScatterSender<T> GetScatterSender<T>(string operatorName)
        {
            return GetOperatorInstance<ScatterSender<T>>(operatorName);
        }

        /// <summary>
        /// Gets the ScatterReceiver with the given name and message type.
        /// </summary>
        /// <typeparam name="T">The message type</typeparam>
        /// <param name="operatorName">The name of the Scatter operator</param>
        /// <returns>The ScatterReceiver</returns>
        public IScatterReceiver<T> GetScatterReceiver<T>(string operatorName)
        {
            return GetOperatorInstance<ScatterReceiver<T>>(operatorName);
        }

        /// <summary>
        /// Gets the Group Communication operator with the specified name and type.
        /// If the operator hasn't been instanciated yet, find the injector 
        /// associated with the given operator name and use the type information 
        /// to create a new operator of that type.
        /// </summary>
        /// <typeparam name="T">The type of operator to create</typeparam>
        /// <param name="operatorName">The name of the operator</param>
        /// <returns>The newly created Group Communication Operator</returns>
        private T GetOperatorInstance<T>(string operatorName) where T : class
        {
            if (string.IsNullOrEmpty(operatorName))
            {
                throw new ArgumentNullException("operatorName");
            }

            object op;
            if (!_operators.TryGetValue(operatorName, out op))
            {
                Exceptions.Throw(new ArgumentException("Operator is not added at Driver side:" + operatorName), LOGGER);
            }

            return (T)op;
        }

        /// <summary>
        /// Call each Operator to ensure all the nodes in the topology group has been registered
        /// </summary>
        void ICommunicationGroupClientInternal.WaitingForRegistration()
        {
            foreach (var op in _operators.Values)
            {
                var method = op.GetType().GetMethod("Org.Apache.REEF.Network.Group.Operators.IGroupCommOperatorInternal.WaitForRegistration", BindingFlags.NonPublic | BindingFlags.Instance);
                method.Invoke(op, null);
            }
        }
    }
}
