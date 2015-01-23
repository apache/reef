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
using Org.Apache.Reef.IO.Network.Group.Operators;
using Org.Apache.Reef.IO.Network.Group.Operators.Impl;
using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Tang.Implementations;
using Org.Apache.Reef.Tang.Interface;
using Org.Apache.Reef.Tang.Util;
using Org.Apache.Reef.Wake.Remote;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Org.Apache.Reef.IO.Network.Group.Topology
{
    /// <summary>
    /// Represents a graph of MPI Operators where there are only two levels of
    /// nodes: the root and all children extending from the root.
    /// </summary>
    /// <typeparam name="T">The message type</typeparam>
    public class FlatTopology<T> : ITopology<T>
    {
        private string _groupName;
        private string _operatorName;

        private string _rootId;
        private string _driverId;

        private Dictionary<string, TaskNode> _nodes;
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
            IOperatorSpec<T> operatorSpec)
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
        public IOperatorSpec<T> OperatorSpec { get; set; }

        /// <summary>
        /// Gets the task configuration for the operator topology.
        /// </summary>
        /// <param name="taskId">The task identifier</param>
        /// <returns>The task configuration</returns>
        public IConfiguration GetTaskConfiguration(string taskId)
        {
            var confBuilder = TangFactory.GetTang().NewConfigurationBuilder()
                .BindImplementation(typeof(ICodec<T>), OperatorSpec.Codec.GetType())
                .BindNamedParameter<MpiConfigurationOptions.TopologyRootTaskId, string>(
                    GenericType<MpiConfigurationOptions.TopologyRootTaskId>.Class,
                    _rootId);

            foreach (string tId in _nodes.Keys)
            {
                if (!tId.Equals(_rootId))
                {
                    confBuilder.BindSetEntry<MpiConfigurationOptions.TopologyChildTaskIds, string>(
                        GenericType<MpiConfigurationOptions.TopologyChildTaskIds>.Class,
                        tId);
                }
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
            TaskNode rootNode = new TaskNode(_groupName, _operatorName, rootId, _driverId);
            _root = rootNode;

            foreach (TaskNode childNode in _nodes.Values)
            {
                rootNode.AddChild(childNode);
                childNode.SetParent(rootNode);
            }
        }

        private void AddChild(string childId)
        {
            TaskNode childNode = new TaskNode(_groupName, _operatorName, childId, _driverId);
            _nodes[childId] = childNode;

            if (_root != null)
            {
                _root.AddChild(childNode);
                childNode.SetParent(_root);
            }
        }
    }
}
