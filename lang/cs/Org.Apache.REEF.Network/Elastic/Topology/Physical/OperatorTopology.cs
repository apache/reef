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

using Org.Apache.REEF.Network.Elastic.Task;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Topology.Physical
{
    /// <summary>
    /// Base class for task-side topologies. Task-side topologies are
    /// not generic but directly related to the operators using them to communicate data.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public abstract class OperatorTopology : INodeIdentifier
    {
        /// <summary>
        /// Constructor for an operator topology.
        /// </summary>
        /// <param name="stageName">The stage name the topology is working on</param>
        /// <param name="taskId">The identifier of the task the topology is running on</param>
        /// <param name="rootTaskId">The identifier of the root note in the topology</param>
        /// <param name="operatorId">The identifier of the operator for this topology</param>
        public OperatorTopology(string stageName, string taskId, string rootTaskId, int operatorId)
        {
            StageName = stageName;
            TaskId = taskId;
            RootTaskId = rootTaskId;
            OperatorId = operatorId;
        }

        /// <summary>
        /// The stage name context in which the topology is running.
        /// </summary>
        public string StageName { get; private set; }

        /// <summary>
        /// The identifier of the operator in which the topology is running.
        /// </summary>
        public int OperatorId { get; private set; }

        /// <summary>
        /// The identifier of the task in which the topology is running.
        /// </summary>
        protected string TaskId { get; private set; }

        /// <summary>
        /// The task identifier of the root node of the topology.
        /// </summary>
        protected string RootTaskId { get; set; }

        /// <summary>
        /// Waiting logic before disposing topologies. 
        /// </summary>
        public virtual void WaitCompletionBeforeDisposing()
        {
        }
    }
}
