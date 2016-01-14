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

namespace Org.Apache.REEF.Network.Group.Topology
{
    /// <summary>
    /// Represents a node in the operator topology graph.
    /// </summary>
    internal sealed class TaskNode
    {
        private string _groupName;
        private string _operatorName;
        private string _taskId;
        private string _driverId;

        private TaskNode _parent;
        private TaskNode _successor;
        private bool _isRoot;

        private readonly List<TaskNode> _children;

        public TaskNode(
            string groupName, 
            string operatorName, 
            string taskId, 
            string driverId,
            bool isRoot)
        {
            _groupName = groupName;
            _operatorName = operatorName;
            _taskId = taskId;
            _driverId = driverId;
            _isRoot = isRoot;

            _children = new List<TaskNode>();
        }

        public string TaskId
        {
            get { return _taskId; } 
            private set { _taskId = value;  }
        }

        public TaskNode Successor
        {
            get { return _successor; }
            set { _successor = value; }
        }

        public TaskNode Parent
        {
            get { return _parent; }
            set { _parent = value; }
        }

        public void AddChild(TaskNode child)
        {
            _children.Add(child);
        }

        public int GetNumberOfChildren() 
        {
            return _children.Count;
        }

        public IList<TaskNode> GetChildren()
        {
            return _children;
        } 
    }
}
