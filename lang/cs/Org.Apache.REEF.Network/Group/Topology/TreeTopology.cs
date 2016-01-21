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
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Network.Group.Operators.Impl;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Tang.Implementations.Configuration;

namespace Org.Apache.REEF.Network.Group.Topology
{
    internal sealed class TreeTopology<T> : ITopology<T> 
    {
        private readonly string _groupName;
        private readonly string _operatorName;

        private readonly string _rootId;
        private readonly string _driverId;

        private readonly Dictionary<string, TaskNode> _nodes;
        private TaskNode _root;
        private TaskNode _logicalRoot;
        private TaskNode _prev;

        private readonly int _fanOut;

        /// <summary>
        /// Creates a new TreeTopology.
        /// </summary>
        /// <param name="operatorName">The operator name</param>
        /// <param name="groupName">The name of the topology's CommunicationGroup</param>
        /// <param name="rootId">The root Task identifier</param>
        /// <param name="driverId">The driver identifier</param>
        /// <param name="operatorSpec">The operator specification</param>
        /// <param name="fanOut">The number of children for a tree node</param>
        public TreeTopology(
            string operatorName, 
            string groupName, 
            string rootId,
            string driverId,
            IOperatorSpec operatorSpec,
            int fanOut)
        {
            _groupName = groupName;
            _operatorName = operatorName;
            _rootId = rootId;
            _driverId = driverId;

            OperatorSpec = operatorSpec;
            _fanOut = fanOut;

            _nodes = new Dictionary<string, TaskNode>(); 
        }

        public IOperatorSpec OperatorSpec { get; set; }

        /// <summary>
        /// Gets the task configuration for the operator topology.
        /// </summary>
        /// <param name="taskId">The task identifier</param>
        /// <returns>The task configuration</returns>
        public IConfiguration GetTaskConfiguration(string taskId)
        {
            if (taskId == null)
            {
                throw new ArgumentException("TaskId is null when GetTaskConfiguration");
            }

            TaskNode selfTaskNode = GetTaskNode(taskId);
            if (selfTaskNode == null)
            {
                throw new ArgumentException("Task has not been added to the topology");
            }

            string parentId;
            TaskNode parent = selfTaskNode.Parent;
            if (parent == null)
            {
                parentId = selfTaskNode.TaskId;
            }
            else
            {
                parentId = parent.TaskId;
            }

            // add parentid, if no parent, add itself
            ICsConfigurationBuilder confBuilder = TangFactory.GetTang().NewConfigurationBuilder()
                .BindNamedParameter<GroupCommConfigurationOptions.TopologyRootTaskId, string>(
                    GenericType<GroupCommConfigurationOptions.TopologyRootTaskId>.Class,
                    parentId);

            // add all its children
            foreach (TaskNode childNode in selfTaskNode.GetChildren())
            {
                confBuilder.BindSetEntry<GroupCommConfigurationOptions.TopologyChildTaskIds, string>(
                    GenericType<GroupCommConfigurationOptions.TopologyChildTaskIds>.Class,
                    childNode.TaskId);
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

        public void AddTask(string taskId)
        {
            if (string.IsNullOrEmpty(taskId))
            {
                throw new ArgumentNullException("taskId");
            }
            if (_nodes.ContainsKey(taskId))
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

        private TaskNode GetTaskNode(string taskId)
        {
            TaskNode n;
            if (_nodes.TryGetValue(taskId, out n))
            {
                return n;
            }
            throw new ArgumentException("cannot find task node in the nodes.");
        }

        private void AddChild(string taskId)
        {
            TaskNode node = new TaskNode(_groupName, _operatorName, taskId, _driverId, false);
            if (_logicalRoot != null)
            {
                AddTaskNode(node);
            }
            _nodes[taskId] = node;
        }

        private void SetRootNode(string rootId) 
        {
            _root = new TaskNode(_groupName, _operatorName, rootId, _driverId, true);
            _logicalRoot = _root;
            _prev = _root;

            foreach (TaskNode n in _nodes.Values) 
            {
                AddTaskNode(n);
            }
            _nodes[rootId] = _root;
        }

        private void AddTaskNode(TaskNode node) 
        {
            if (_logicalRoot.GetNumberOfChildren() >= _fanOut) 
            {
                _logicalRoot = _logicalRoot.Successor;
            }
            node.Parent = _logicalRoot;
            _logicalRoot.AddChild(node);
            _prev.Successor = node;
            _prev = node;
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
    }
}
