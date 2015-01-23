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
using Org.Apache.Reef.IO.Network.Group.Operators;
using Org.Apache.Reef.IO.Network.Group.Operators.Impl;
using Org.Apache.Reef.IO.Network.NetworkService;
using Org.Apache.Reef.Tasks;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Tang.Exceptions;
using Org.Apache.Reef.Tang.Formats;
using Org.Apache.Reef.Tang.Implementations;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Util;
using System;
using System.Collections.Generic;

namespace Org.Apache.Reef.IO.Network.Group.Task.Impl
{
    /// <summary>
    ///  Used by Tasks to fetch MPI Operators in the group configured by the driver.
    /// </summary>
    public class CommunicationGroupClient : ICommunicationGroupClient
    {
        private readonly Logger LOGGER = Logger.GetLogger(typeof(CommunicationGroupClient));

        private string _taskId;
        private string _driverId;

        private Dictionary<string, IInjector> _operatorInjectors; 
        private Dictionary<string, object> _operators;
        private NetworkService<GroupCommunicationMessage> _networkService; 
        private IMpiNetworkObserver _mpiNetworkHandler;
        private ICommunicationGroupNetworkObserver _commGroupNetworkHandler;

        /// <summary>
        /// Creates a new CommunicationGroupClient.
        /// </summary>
        /// <param name="taskId">The identifier for this Task.</param>
        /// <param name="groupName">The name of the CommunicationGroup</param>
        /// <param name="driverId">The identifier for the driver</param>
        /// <param name="operatorConfigs">The serialized operator configurations</param>
        /// <param name="mpiNetworkObserver">The handler for all incoming messages
        /// across all Communication Groups</param>
        /// <param name="networkService">The network service used to send messages.</param>
        /// <param name="configSerializer">Used to deserialize operator configuration.</param>
        [Inject]
        public CommunicationGroupClient(
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            [Parameter(typeof(MpiConfigurationOptions.CommunicationGroupName))] string groupName,
            [Parameter(typeof(MpiConfigurationOptions.DriverId))] string driverId,
            [Parameter(typeof(MpiConfigurationOptions.SerializedOperatorConfigs))] ISet<string> operatorConfigs,
            IMpiNetworkObserver mpiNetworkObserver,
            NetworkService<GroupCommunicationMessage> networkService,
            AvroConfigurationSerializer configSerializer)
        {
            _taskId = taskId;
            _driverId = driverId;
            GroupName = groupName;

            _operators = new Dictionary<string, object>();
            _operatorInjectors = new Dictionary<string, IInjector>();

            _networkService = networkService;
            _mpiNetworkHandler = mpiNetworkObserver;
            _commGroupNetworkHandler = new CommunicationGroupNetworkObserver();
            _mpiNetworkHandler.Register(groupName, _commGroupNetworkHandler);

            // Deserialize operator configuration and store each injector.
            // When user requests the MPI Operator, use type information to
            // create the instance.
            foreach (string operatorConfigStr in operatorConfigs)
            {
                IConfiguration operatorConfig = configSerializer.FromString(operatorConfigStr);

                IInjector injector = TangFactory.GetTang().NewInjector(operatorConfig);
                string operatorName = injector.GetNamedInstance<MpiConfigurationOptions.OperatorName, string>(
                    GenericType<MpiConfigurationOptions.OperatorName>.Class);
                _operatorInjectors[operatorName] = injector;
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
        /// Gets the MPI operator with the specified name and type.
        /// If the operator hasn't been instanciated yet, find the injector 
        /// associated with the given operator name and use the type information 
        /// to create a new operator of that type.
        /// </summary>
        /// <typeparam name="T">The type of operator to create</typeparam>
        /// <param name="operatorName">The name of the operator</param>
        /// <returns>The newly created MPI Operator</returns>
        private T GetOperatorInstance<T>(string operatorName) where T : class
        {
            if (string.IsNullOrEmpty(operatorName))
            {
                throw new ArgumentNullException("operatorName");
            }
            if (!_operatorInjectors.ContainsKey(operatorName))
            {
                throw new ArgumentException("Invalid operator name, cannot create CommunicationGroupClient");
            }

            object op;
            if (!_operators.TryGetValue(operatorName, out op))
            {
                IInjector injector = _operatorInjectors[operatorName];

                injector.BindVolatileParameter(GenericType<TaskConfigurationOptions.Identifier>.Class, _taskId);
                injector.BindVolatileParameter(GenericType<MpiConfigurationOptions.CommunicationGroupName>.Class, GroupName);
                injector.BindVolatileInstance(GenericType<ICommunicationGroupNetworkObserver>.Class, _commGroupNetworkHandler);
                injector.BindVolatileInstance(GenericType<NetworkService<GroupCommunicationMessage>>.Class, _networkService);
                injector.BindVolatileInstance(GenericType<ICommunicationGroupClient>.Class, this);

                try
                {
                    op = injector.GetInstance<T>();
                    _operators[operatorName] = op;
                }
                catch (InjectionException)
                {
                    LOGGER.Log(Level.Error, "Cannot inject MPI operator: No known operator of type: {0}", typeof(T));
                    throw;
                }
            }

            return (T) op;
        }
    }
}
