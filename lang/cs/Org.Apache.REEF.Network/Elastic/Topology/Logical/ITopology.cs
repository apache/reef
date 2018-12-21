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

using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Tang.Interface;
using System.Collections.Generic;
using Org.Apache.REEF.Network.Elastic.Failures;
using System;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Topology.Logical
{
    /// <summary>
    /// Represents a topology graph for Elastic Group Communication Operators.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public interface ITopology
    {
        /// <summary>
        /// The identifier of the operator using the topology.
        /// </summary>
        int OperatorId { get; set; }

        /// <summary>
        /// The subscription of the operator using the topology.
        /// </summary>
        string SubscriptionName { get; set; }

        /// <summary>
        /// Adds a new task to the topology.
        /// When called before Build() actually adds the task to the topology.
        /// After Build(), it assumes that the task is added because recovered from a failure.
        /// A failure machine is given as input so that the topology can update the number of available nodes.
        /// </summary>
        /// <param name="taskId">The id of the task to be added</param>
        /// <param name="failureMachine">The failure machine that manage the failure for the operator.</param>
        /// <returns>True if is the first time the topology sees this task</returns>
        bool AddTask(string taskId, IFailureStateMachine failureMachine);

        /// <summary>
        /// Removes a task from the topology.
        /// </summary>
        /// <param name="taskId">The id of the task to be removed</param>
        /// <returns>The number of data points lost because of the removed task</returns>
        int RemoveTask(string taskId);

        /// <summary>
        /// Whether the topology can be sheduled.
        /// </summary>
        /// <returns>True if the topology is ready to be scheduled</returns>
        bool CanBeScheduled();

        /// <summary>
        /// Finalizes the topology.
        /// After the topology has been finalized, any task added to the topology is
        /// assumed as a task recovered from a failure.
        /// </summary>
        /// <returns>The same finalized topology</returns>
        ITopology Build();

        /// <summary>
        /// Adds the topology configuration for the input task to the input builder.
        /// Must be called only after all tasks have been added to the topology, i.e., after build.
        /// </summary>
        /// <param name="builder">The configuration builder the configuration will be appended to</param>
        /// <param name="taskId">The task id of the task that belongs to this Topology</param>
        void GetTaskConfiguration(ref ICsConfigurationBuilder builder, int taskId);

        /// <summary>
        /// Utility method for logging the topology state.
        /// This will be called every time a topology object is built or modified
        /// because of a failure.
        /// </summary>
        string LogTopologyState();

        /// <summary>
        /// This method is triggered when a node detects a change in the topology and asks the driver for an update.
        /// </summary>
        /// <param name="taskId">The identifier of the task asking for the update</param>
        /// <param name="returnMessages">A list of message containing the topology update</param>
        /// <param name="failureStateMachine">An optional failure machine to log updates</param>
        void TopologyUpdateResponse(string taskId, ref List<IElasticDriverMessage> returnMessages, Optional<IFailureStateMachine> failureStateMachine);

        /// <summary>
        /// Action to trigger when the operator recdeives a notification that a new iteration is started.
        /// </summary>
        /// <param name="iteration">The new iteration number</param>
        void OnNewIteration(int iteration);

        /// <summary>
        /// Reconfigure the topology in response to some event.
        /// </summary>
        /// <param name="taskId">The task id responsible for the topology change</param>
        /// <param name="info">Some additional topology-specific information</param>
        /// <param name="iteration">The optional iteration number in which the event occurred</param>
        /// <returns>One or more messages for reconfiguring the Tasks</returns>
        IList<IElasticDriverMessage> Reconfigure(string taskId, Optional<string> info, Optional<int> iteration);

        /// <summary>
        /// Log the final statistics of the operator.
        /// This is called when the pipeline execution is completed.
        /// </summary>
        string LogFinalStatistics();
    }
}
