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
    public class TreeTopology<T1, T2> : ITopology<T1, T2> where T2 : ICodec<T1>
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

        /// <summary>
        /// Creates a new TreeTopology.
        /// </summary>
        /// <param name="operatorName">The operator name</param>
        /// <param name="groupName">The name of the topology's CommunicationGroup</param>
        /// <param name="rootId">The root Task identifier</param>
        /// <param name="driverId">The driver identifier</param>
        /// <param name="operatorSpec">The operator specification</param>
        /// <param name="fanOut">The number of chldren for a tree node</param>
        public TreeTopology(
            string operatorName, 
            string groupName, 
            string rootId,
            string driverId,
            IOperatorSpec<T1, T2> operatorSpec,
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

        public IOperatorSpec<T1, T2> OperatorSpec { get; set; }

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

            //add parentid, if no parent, add itself
            var confBuilder = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(typeof(ICodec<T1>), OperatorSpec.Codec)
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

            if (OperatorSpec is BroadcastOperatorSpec<T1, T2>)
            {
                BroadcastOperatorSpec<T1, T2> broadcastSpec = OperatorSpec as BroadcastOperatorSpec<T1, T2>;
                if (taskId.Equals(broadcastSpec.SenderId))
                {
                    confBuilder.BindImplementation(GenericType<IMpiOperator<T1>>.Class, GenericType<BroadcastSender<T1>>.Class);
                }
                else
                {
                    confBuilder.BindImplementation(GenericType<IMpiOperator<T1>>.Class, GenericType<BroadcastReceiver<T1>>.Class);
                }
            }
            else if (OperatorSpec is ReduceOperatorSpec<T1, T2>)
            {
                ReduceOperatorSpec<T1, T2> reduceSpec = OperatorSpec as ReduceOperatorSpec<T1, T2>;
                confBuilder.BindImplementation(typeof(IReduceFunction<T1>), reduceSpec.ReduceFunction.GetType());

                if (taskId.Equals(reduceSpec.ReceiverId))
                {
                    confBuilder.BindImplementation(GenericType<IMpiOperator<T1>>.Class, GenericType<ReduceReceiver<T1>>.Class);
                }
                else
                {
                    confBuilder.BindImplementation(GenericType<IMpiOperator<T1>>.Class, GenericType<ReduceSender<T1>>.Class);
                }
            }
            else if (OperatorSpec is ScatterOperatorSpec<T1, T2>)
            {
                ScatterOperatorSpec<T1, T2> scatterSpec = OperatorSpec as ScatterOperatorSpec<T1, T2>;
                if (taskId.Equals(scatterSpec.SenderId))
                {
                    confBuilder.BindImplementation(GenericType<IMpiOperator<T1>>.Class, GenericType<ScatterSender<T1>>.Class);
                }
                else
                {
                    confBuilder.BindImplementation(GenericType<IMpiOperator<T1>>.Class, GenericType<ScatterReceiver<T1>>.Class);
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
    }
}
