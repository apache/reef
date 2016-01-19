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
using System.Reflection;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Operators.Impl;
using Org.Apache.REEF.Network.Group.Pipelining.Impl;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Group.Driver.Impl
{
    /// <summary>
    /// Used to configure Group Communication operators in Reef driver.
    /// All operators in the same Communication Group run on the the 
    /// same set of tasks.
    /// </summary>
    public sealed class CommunicationGroupDriver : ICommunicationGroupDriver
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(CommunicationGroupDriver));

        private readonly string _groupName;
        private readonly string _driverId;
        private readonly int _numTasks;
        private int _tasksAdded;
        private bool _finalized;
        private readonly int _fanOut;

        private readonly AvroConfigurationSerializer _confSerializer;

        private readonly object _topologyLock;
        private readonly Dictionary<string, object> _operatorSpecs;
        private readonly Dictionary<string, object> _topologies;

        /// <summary>
        /// Create a new CommunicationGroupDriver.
        /// </summary>
        /// <param name="groupName">The communication group name</param>
        /// <param name="driverId">Identifier of the Reef driver</param>
        /// <param name="numTasks">The number of tasks each operator will use</param>
        /// <param name="confSerializer">Used to serialize task configuration</param>
        public CommunicationGroupDriver(
            string groupName,
            string driverId,
            int numTasks,
            int fanOut,
            AvroConfigurationSerializer confSerializer)
        {
            _confSerializer = confSerializer;
            _groupName = groupName;
            _driverId = driverId;
            _numTasks = numTasks;
            _tasksAdded = 0;
            _finalized = false;
            _fanOut = fanOut;

            _topologyLock = new object();

            _operatorSpecs = new Dictionary<string, object>();
            _topologies = new Dictionary<string, object>();
            TaskIds = new List<string>();
        }

        /// <summary>
        /// Returns the list of task ids that belong to this Communication Group
        /// </summary>
        public List<string> TaskIds { get; private set; }

        /// <summary>
        /// </summary>
        /// <typeparam name="T">The type of messages that operators will send</typeparam>
        /// <param name="configurations">The configuration send to Evaluator</param>
        /// <param name="operatorName">The name of the broadcast operator</param>
        /// <param name="masterTaskId">The master task id in broadcast operator</param>
        /// <param name="topologyType">The topology type for the operator</param>
        /// <returns>The same CommunicationGroupDriver with the added Broadcast operator info</returns>
        /// <returns></returns>
        public ICommunicationGroupDriver AddBroadcast<T>(string operatorName, string masterTaskId, TopologyTypes topologyType, params IConfiguration[] configurations)
        {
            if (_finalized)
            {
                throw new IllegalStateException("Can't add operators once the spec has been built.");
            }

            var spec = new BroadcastOperatorSpec(
                masterTaskId,
                configurations);

            ITopology<T> topology;
            if (topologyType == TopologyTypes.Flat)
            {
                topology = new FlatTopology<T>(operatorName, _groupName, spec.SenderId, _driverId,
                    spec);
            }
            else
            {
                topology = new TreeTopology<T>(operatorName, _groupName, spec.SenderId, _driverId,
                    spec,
                    _fanOut);
            }

            _topologies[operatorName] = topology;
            _operatorSpecs[operatorName] = spec;

            return this;
        }

        /// <summary>
        /// Adds the Broadcast Group Communication operator to the communication group. Default to Int message type
        /// </summary>
        /// <param name="operatorName">The name of the broadcast operator</param>
        /// <param name="masterTaskId">The master task id in broadcast operator</param>
        /// <param name="topologyType">The topology type for the operator</param>
        /// <returns>The same CommunicationGroupDriver with the added Broadcast operator info</returns>
        public ICommunicationGroupDriver AddBroadcast(string operatorName, string masterTaskId, TopologyTypes topologyType = TopologyTypes.Flat)
        {
            return AddBroadcast<int>(operatorName, masterTaskId, topologyType, GetDefaultConfiguration());
        }

        /// <summary>
        /// Adds the Reduce Group Communication operator to the communication group.
        /// </summary>
        /// <typeparam name="T">The type of messages that operators will send</typeparam>
        /// <param name="configurations">The configuration for the reduce operator</param>
        /// <param name="operatorName">The name of the reduce operator</param>
        /// <param name="masterTaskId">The master task id for the typology</param>
        /// <param name="topologyType">The topology for the operator</param>
        /// <returns>The same CommunicationGroupDriver with the added Reduce operator info</returns>
        public ICommunicationGroupDriver AddReduce<T>(
            string operatorName,
            string masterTaskId,
            TopologyTypes topologyType,
            params IConfiguration[] configurations) 
        {
            if (_finalized)
            {
                throw new IllegalStateException("Can't add operators once the spec has been built.");
            }

            var spec = new ReduceOperatorSpec(
                masterTaskId,
                configurations);

            ITopology<T> topology;

            if (topologyType == TopologyTypes.Flat)
            {
                topology = new FlatTopology<T>(operatorName, _groupName, spec.ReceiverId,
                    _driverId, spec);
            }
            else
            {
                topology = new TreeTopology<T>(operatorName, _groupName, spec.ReceiverId,
                    _driverId, spec,
                    _fanOut);
            }

            _topologies[operatorName] = topology;
            _operatorSpecs[operatorName] = spec;

            return this;
        }

        /// <summary>
        /// Adds the Scatter Group Communication operator to the communication group.
        /// </summary>
        /// <typeparam name="T">The type of messages that operators will send</typeparam>
        /// <param name="configurations">The configuration for the scatter operator</param>
        /// <param name="operatorName">The name of the scatter operator</param>
        /// <param name="senderId">The sender id</param>
        /// <param name="topologyType">type of topology used in the operaor</param>
        /// <returns>The same CommunicationGroupDriver with the added Scatter operator info</returns>
        public ICommunicationGroupDriver AddScatter<T>(string operatorName, string senderId,
            TopologyTypes topologyType, params IConfiguration[] configurations) 
        {
            if (_finalized)
            {
                throw new IllegalStateException("Can't add operators once the spec has been built.");
            }

            var spec = new ScatterOperatorSpec(senderId, configurations);

            ITopology<T> topology;

            if (topologyType == TopologyTypes.Flat)
            {
                topology = new FlatTopology<T>(operatorName, _groupName, spec.SenderId, _driverId,
                    spec);
            }
            else
            {
                topology = new TreeTopology<T>(operatorName, _groupName, spec.SenderId, _driverId,
                    spec,
                    _fanOut);
            }
            _topologies[operatorName] = topology;
            _operatorSpecs[operatorName] = spec;

            return this;
        }

        /// <summary>
        /// Adds the Scatter Group Communication operator to the communication group with default Int message type
        /// </summary>
        /// <param name="operatorName">The name of the scatter operator</param>
        /// <param name="senderId">The sender id</param>
        /// <param name="topologyType">type of topology used in the operaor</param>
        /// <returns>The same CommunicationGroupDriver with the added Scatter operator info</returns>
        public ICommunicationGroupDriver AddScatter(string operatorName, string senderId,
            TopologyTypes topologyType = TopologyTypes.Flat)
        {
            return AddScatter<int>(operatorName, senderId, topologyType, GetDefaultConfiguration());
        }

        /// <summary>
        /// Finalizes the CommunicationGroupDriver.
        /// After the CommunicationGroupDriver has been finalized, no more operators may
        /// be added to the group.
        /// </summary>
        /// <returns>The same finalized CommunicationGroupDriver</returns>
        public ICommunicationGroupDriver Build()
        {
            _finalized = true;
            return this;
        }

        /// <summary>
        /// Add a task to the communication group.
        /// The CommunicationGroupDriver must have called Build() before adding tasks to the group.
        /// </summary>
        /// <param name="taskId">The id of the task to add</param>
        public void AddTask(string taskId)
        {
            if (!_finalized)
            {
                throw new IllegalStateException(
                    "CommunicationGroupDriver must call Build() before adding tasks to the group.");
            }

            lock (_topologyLock)
            {
                _tasksAdded++;
                if (_tasksAdded > _numTasks)
                {
                    throw new IllegalStateException("Added too many tasks to Communication Group, expected: " +
                                                    _numTasks);
                }

                TaskIds.Add(taskId);
                foreach (string operatorName in _operatorSpecs.Keys)
                {
                    AddTask(operatorName, taskId);
                }
            }
        }

        /// <summary>
        /// Get the Task Configuration for this communication group. 
        /// Must be called only after all tasks have been added to the CommunicationGroupDriver.
        /// </summary>
        /// <param name="taskId">The task id of the task that belongs to this Communication Group</param>
        /// <returns>The Task Configuration for this communication group</returns>
        public IConfiguration GetGroupTaskConfiguration(string taskId)
        {
            if (!TaskIds.Contains(taskId))
            {
                return null;
            }

            // Make sure all tasks have been added to communication group before generating config
            lock (_topologyLock)
            {
                if (_tasksAdded != _numTasks)
                {
                    throw new IllegalStateException(
                        "Must add all tasks to communication group before fetching configuration");
                }
            }

            var confBuilder = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<GroupCommConfigurationOptions.DriverId, string>(
                    GenericType<GroupCommConfigurationOptions.DriverId>.Class,
                    _driverId)
                .BindNamedParameter<GroupCommConfigurationOptions.CommunicationGroupName, string>(
                    GenericType<GroupCommConfigurationOptions.CommunicationGroupName>.Class,
                    _groupName);

            foreach (var operatorName in _topologies.Keys)
            {
                var innerConf =
                    TangFactory.GetTang().NewConfigurationBuilder(GetOperatorConfiguration(operatorName, taskId))
                        .BindNamedParameter<GroupCommConfigurationOptions.OperatorName, string>(
                            GenericType<GroupCommConfigurationOptions.OperatorName>.Class,
                            operatorName)
                        .Build();

                confBuilder.BindSetEntry<GroupCommConfigurationOptions.SerializedOperatorConfigs, string>(
                    GenericType<GroupCommConfigurationOptions.SerializedOperatorConfigs>.Class,
                    _confSerializer.ToString(innerConf));
            }

            return confBuilder.Build();
        }

        private void AddTask(string operatorName, string taskId)
        {
            var topology = _topologies[operatorName];
            MethodInfo info = topology.GetType().GetMethod("AddTask");
            info.Invoke(topology, new[] { (object)taskId });
        }

        private IConfiguration GetOperatorConfiguration(string operatorName, string taskId)
        {
            var topology = _topologies[operatorName];
            MethodInfo info = topology.GetType().GetMethod("GetTaskConfiguration");
            return (IConfiguration)info.Invoke(topology, new[] { (object)taskId });
        }

        private IConfiguration[] GetDefaultConfiguration()
        {
            List<IConfiguration> list = new List<IConfiguration>(); 

            IConfiguration dataConverterConfig = PipelineDataConverterConfiguration<int>.Conf
                .Set(PipelineDataConverterConfiguration<int>.DataConverter, GenericType<DefaultPipelineDataConverter<int>>.Class)
                .Build();

            list.Add(dataConverterConfig);

            return list.ToArray();
        }
    }
}