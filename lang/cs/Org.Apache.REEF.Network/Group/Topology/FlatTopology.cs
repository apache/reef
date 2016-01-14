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
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Operators.Impl;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Tang.Implementations.Configuration;

namespace Org.Apache.REEF.Network.Group.Topology
{
    /// <summary>
    /// Represents a graph of Group Communication Operators where there are only two levels of
    /// nodes: the root and all children extending from the root.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public sealed class FlatTopology<T> : ITopology<T> 
    {
        private readonly string _groupName;
        private readonly string _operatorName;

        private readonly string _rootId;
        private readonly string _driverId;

        private readonly Dictionary<string, TaskNode> _nodes;
        private TaskNode _root;

        /// <summary>
        /// Creates a new FlatTopology.
        /// </summary>
        /// <param name="operatorName">The operator name</param>
        /// <param name="groupName">The name of the topology's CommunicationGroup</param>
        /// <param name="rootId">The root Task identifier</param>
        /// <param name="driverId">The driver identifier</param>
        /// <param name="operatorSpec">The operator specification</param>
        public FlatTopology(
            string operatorName, 
            string groupName, 
            string rootId,
            string driverId,
            IOperatorSpec operatorSpec)
        {
            _groupName = groupName;
            _operatorName = operatorName;
            _rootId = rootId;
            _driverId = driverId;

            OperatorSpec = operatorSpec;

            _nodes = new Dictionary<string, TaskNode>(); 
        }

        /// <summary>
        /// Gets the Operator specification
        /// </summary>
        public IOperatorSpec OperatorSpec { get; set; }

        /// <summary>
        /// Gets the task configuration for the operator topology.
        /// </summary>
        /// <param name="taskId">The task identifier</param>
        /// <returns>The task configuration</returns>
        public IConfiguration GetTaskConfiguration(string taskId)
        {
            ICsConfigurationBuilder confBuilder;
            confBuilder = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<GroupCommConfigurationOptions.TopologyRootTaskId, string>(
                GenericType<GroupCommConfigurationOptions.TopologyRootTaskId>.Class,
                _rootId);

            if (taskId.Equals(_rootId))
            {
                foreach (var tId in _nodes.Keys)
                {
                    if (!tId.Equals(_rootId))
                    {
                        confBuilder.BindSetEntry<GroupCommConfigurationOptions.TopologyChildTaskIds, string>(
                            GenericType<GroupCommConfigurationOptions.TopologyChildTaskIds>.Class,
                            tId);
                    }
                }
            }

            if (OperatorSpec is BroadcastOperatorSpec)
            {
                var broadcastSpec = OperatorSpec as BroadcastOperatorSpec;

                if (taskId.Equals(broadcastSpec.SenderId))
                {
                    confBuilder.BindImplementation(GenericType<IGroupCommOperator<T>>.Class, GenericType<BroadcastSender<T>>.Class);
                    SetMessageType(typeof(BroadcastSender<T>), confBuilder);
                }
                else
                {
                    confBuilder.BindImplementation(GenericType<IGroupCommOperator<T>>.Class, GenericType<BroadcastReceiver<T>>.Class);
                    SetMessageType(typeof(BroadcastReceiver<T>), confBuilder);
                }
            }
            else if (OperatorSpec is ReduceOperatorSpec)
            {
                var reduceSpec = OperatorSpec as ReduceOperatorSpec;
                if (taskId.Equals(reduceSpec.ReceiverId))
                {
                    confBuilder.BindImplementation(GenericType<IGroupCommOperator<T>>.Class, GenericType<ReduceReceiver<T>>.Class);
                    SetMessageType(typeof(ReduceReceiver<T>), confBuilder);
                }
                else
                {
                    confBuilder.BindImplementation(GenericType<IGroupCommOperator<T>>.Class, GenericType<ReduceSender<T>>.Class);
                    SetMessageType(typeof(ReduceSender<T>), confBuilder);
                }
            }
            else if (OperatorSpec is ScatterOperatorSpec)
            {
                ScatterOperatorSpec scatterSpec = OperatorSpec as ScatterOperatorSpec;
                if (taskId.Equals(scatterSpec.SenderId))
                {
                    confBuilder.BindImplementation(GenericType<IGroupCommOperator<T>>.Class, GenericType<ScatterSender<T>>.Class);
                    SetMessageType(typeof(ScatterSender<T>), confBuilder);
                }
                else
                {
                    confBuilder.BindImplementation(GenericType<IGroupCommOperator<T>>.Class, GenericType<ScatterReceiver<T>>.Class);
                    SetMessageType(typeof(ScatterReceiver<T>), confBuilder);
                }
            }
            else
            {
                throw new NotSupportedException("Spec type not supported");
            }

            return Configurations.Merge(confBuilder.Build(), OperatorSpec.Configuration);
        }

        private static void SetMessageType(Type operatorType, ICsConfigurationBuilder confBuilder)
        {
            if (operatorType.IsGenericType)
            {
                var genericTypes = operatorType.GenericTypeArguments;
                var msgType = genericTypes[0];
                confBuilder.BindNamedParameter<GroupCommConfigurationOptions.MessageType, string>(
                    GenericType<GroupCommConfigurationOptions.MessageType>.Class, msgType.AssemblyQualifiedName);
            }
        }

        /// <summary>
        /// Adds a task to the topology graph.
        /// </summary>
        /// <param name="taskId">The identifier of the task to add</param>
        public void AddTask(string taskId)
        {
            if (string.IsNullOrEmpty(taskId))
            {
                throw new ArgumentNullException("taskId");
            }
            else if (_nodes.ContainsKey(taskId))
            {
                throw new ArgumentException("Task has already been added to the topology");
            }

            if (taskId.Equals(_rootId))
            {
                SetRootNode(_rootId);
            }
            else
            {
                AddChild(taskId);
            }
        }

        private void SetRootNode(string rootId)
        {
            TaskNode rootNode = new TaskNode(_groupName, _operatorName, rootId, _driverId, true);
            _root = rootNode;

            foreach (TaskNode childNode in _nodes.Values)
            {
                rootNode.AddChild(childNode);
                childNode.Parent = rootNode;
            }
        }

        private void AddChild(string childId)
        {
            TaskNode childNode = new TaskNode(_groupName, _operatorName, childId, _driverId, false);
            _nodes[childId] = childNode;

            if (_root != null)
            {
                _root.AddChild(childNode);
                childNode.Parent = _root;
            }
        }
    }
}