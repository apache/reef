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
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.Group.Topology
{
    public class TreeTopology<T> : ITopology<T>
    {
        private readonly string _groupName;
        private readonly string _operatorName;

        private readonly string _rootId;
        private readonly string _driverId;

        private readonly Dictionary<string, TaskNode> _nodes;
        private TaskNode _root;
        private TaskNode _logicalRoot;
        private TaskNode _prev;

        private int _fanOut;

        public TreeTopology(
            string operatorName, 
            string groupName, 
            string rootId,
            string driverId,
            IOperatorSpec<T> operatorSpec,
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

        public IOperatorSpec<T> OperatorSpec { get; set; }

        public IConfiguration GetTaskConfiguration(string taskId)
        {
            TaskNode selfTaskNode = GetTaskNode(taskId);
            if (selfTaskNode == null)
            {
                throw new ArgumentException("Task has not been added to the topology");
            }

            string parentId;
            TaskNode parent = selfTaskNode.GetParent();
            if (parent == null)
            {
                parentId = selfTaskNode.TaskId;
            }
            else
            {
                parentId = parent.TaskId;
            }

            //add parentid, if no parent, add itself
            var confBuilder = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(typeof(ICodec<T>), OperatorSpec.Codec.GetType())
                .BindNamedParameter<MpiConfigurationOptions.TopologyRootTaskId, string>(
                    GenericType<MpiConfigurationOptions.TopologyRootTaskId>.Class,
                    parentId);

            //add all its children
            foreach (TaskNode childNode in selfTaskNode.GetChildren())
            {
                confBuilder.BindSetEntry<MpiConfigurationOptions.TopologyChildTaskIds, string>(
                    GenericType<MpiConfigurationOptions.TopologyChildTaskIds>.Class,
                    childNode.TaskId);
            }

            if (OperatorSpec is BroadcastOperatorSpec<T>)
            {
                BroadcastOperatorSpec<T> broadcastSpec = OperatorSpec as BroadcastOperatorSpec<T>;
                if (taskId.Equals(broadcastSpec.SenderId))
                {
                    confBuilder.BindImplementation(GenericType<IMpiOperator<T>>.Class, GenericType<BroadcastSender<T>>.Class);
                }
                else
                {
                    confBuilder.BindImplementation(GenericType<IMpiOperator<T>>.Class, GenericType<BroadcastReceiver<T>>.Class);
                }
            }
            else if (OperatorSpec is ReduceOperatorSpec<T>)
            {
                ReduceOperatorSpec<T> reduceSpec = OperatorSpec as ReduceOperatorSpec<T>;
                confBuilder.BindImplementation(typeof(IReduceFunction<T>), reduceSpec.ReduceFunction.GetType());

                if (taskId.Equals(reduceSpec.ReceiverId))
                {
                    confBuilder.BindImplementation(GenericType<IMpiOperator<T>>.Class, GenericType<ReduceReceiver<T>>.Class);
                }
                else
                {
                    confBuilder.BindImplementation(GenericType<IMpiOperator<T>>.Class, GenericType<ReduceSender<T>>.Class);
                }
            }
            else if (OperatorSpec is ScatterOperatorSpec<T>)
            {
                ScatterOperatorSpec<T> scatterSpec = OperatorSpec as ScatterOperatorSpec<T>;
                if (taskId.Equals(scatterSpec.SenderId))
                {
                    confBuilder.BindImplementation(GenericType<IMpiOperator<T>>.Class, GenericType<ScatterSender<T>>.Class);
                }
                else
                {
                    confBuilder.BindImplementation(GenericType<IMpiOperator<T>>.Class, GenericType<ScatterReceiver<T>>.Class);
                }
            }
            else
            {
                throw new NotSupportedException("Spec type not supported");
            }

            return confBuilder.Build();
        }

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
            //_prev = GetTaskNode(taskId);
        }

        private TaskNode GetTaskNode(string taskId)
        {
            TaskNode n;
            _nodes.TryGetValue(taskId, out n);
            return n;
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
            TaskNode node = new TaskNode(_groupName, _operatorName, rootId, _driverId, true);
            _root = node;
            _logicalRoot = _root;
            _prev = _root;

            foreach (TaskNode n in _nodes.Values) 
            {
                AddTaskNode(n);
                //_prev = n;
            }
            _nodes[rootId] = _root;
        }

        private void AddTaskNode(TaskNode node) 
        {
            if (_logicalRoot.GetNumberOfChildren() >= _fanOut) 
            {
                _logicalRoot = _logicalRoot.Successor();
            }
            node.SetParent(_logicalRoot);
            _logicalRoot.AddChild(node);
            _prev.SetSibling(node);
            _prev = node;
        }
    }
}
