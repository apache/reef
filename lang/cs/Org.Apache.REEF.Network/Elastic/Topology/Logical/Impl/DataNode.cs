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

using Org.Apache.REEF.Network.Elastic.Topology.Logical.Enum;
using Org.Apache.REEF.Utilities.Attributes;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl
{
    /// <summary>
    /// Represents a node in the operator topology graph.
    /// Nodes are logical representations in the Driver for tasks.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal sealed class DataNode
    {
        private readonly bool _isRoot;
        private readonly List<DataNode> _children;

        /// <summary>
        /// Construct a node using a given task id.
        /// </summary>
        /// <param name="taskId">The id for the node</param>
        /// <param name="isRoot">Whether the node is the root/master of the topology or not</param>
        public DataNode(
            int taskId,
            bool isRoot)
        {
            TaskId = taskId;
            _isRoot = isRoot;
            FailState = DataNodeState.Reachable;

            _children = new List<DataNode>();
        }

        /// <summary>
        /// The current state for the node.
        /// </summary>
        public DataNodeState FailState { get; set; }

        /// <summary>
        /// The parent of the target node.
        /// </summary>
        public DataNode Parent { get; set; }

        /// <summary>
        /// Add a node to the list of children nodes of the current one.
        /// </summary>
        public void AddChild(IEnumerable<DataNode> child)
        {
            _children.AddRange(child);
        }

        /// <summary>
        /// The task id represented by the data node.
        /// </summary>
        public int TaskId { get; }

        /// <summary>
        /// Return how many children the current node has.
        /// </summary>
        public int NumberOfChildren
        {
            get { return _children.Count; }
        }

        /// <summary>
        /// Return the list of children fro the current node.
        /// </summary>
        public IList<DataNode> Children
        {
            get { return _children; }
        }
    }
}
