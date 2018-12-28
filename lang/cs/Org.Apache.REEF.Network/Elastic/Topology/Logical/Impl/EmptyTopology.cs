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

using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Exceptions;
using System.Collections.Generic;
using Org.Apache.REEF.Network.Elastic.Comm;
using System;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl
{
    /// <summary>
    /// Topology with no structure.
    /// Used as a placeholder when no topology is required.
    /// </summary>
    [Unstable("0.16", "API may change")]
    class EmptyTopology : ITopology
    {
        private bool _finalized;

        /// <summary>
        /// Constructor for the empty topology.
        /// </summary>
        public EmptyTopology()
        {
            _finalized = false;
            OperatorId = -1;
        }

        /// <summary>
        /// The identifier of the operator using the topology.
        /// </summary>
        public int OperatorId { get; set; }

        /// <summary>
        /// The stage of the operator using the topology.
        /// </summary>
        public string StageName { get; set; }

        /// <summary>
        /// Adds a new task to the topology.
        /// This method does nothing on the empty topology.
        /// </summary>
        /// <param name="taskId">The id of the task to be added</param>
        /// <param name="failureMachine">The failure machine that manage the failure for the operator.</param>
        /// <returns>This method returns always false</returns>
        public bool AddTask(string taskId, IFailureStateMachine failureMachine)
        {
            return false;
        }

        /// <summary>
        /// Removes a task from the topology.
        /// This method does nothing on the empty topology.
        /// </summary>
        /// <param name="taskId">The id of the task to be removed</param>
        /// <returns>This method return always 0</returns>
        public int RemoveTask(string taskId)
        {
            return 0;
        }

        /// <summary>
        /// Whether the topology can be sheduled.
        /// </summary>
        /// <returns>This method return always true</returns>
        public bool CanBeScheduled()
        {
            return true;
        }

        /// <summary>
        /// Finalizes the topology.
        /// </summary>
        /// <returns>The same finalized topology</returns>
        public ITopology Build()
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("Topology cannot be built more than once");
            }

            if (OperatorId <= 0)
            {
                throw new IllegalStateException("Topology cannot be built because not linked to any operator");
            }

            if (StageName == string.Empty)
            {
                throw new IllegalStateException("Topology cannot be built because not linked to any stage");
            }

            _finalized = true;

            return this;
        }

        /// <summary>
        /// Adds the topology configuration for the input task to the input builder.
        /// This method does nothig.
        /// </summary>
        /// <param name="builder">The configuration builder the configuration will be appended to</param>
        /// <param name="taskId">The task id of the task that belongs to this Topology</param>
        public void GetTaskConfiguration(ref ICsConfigurationBuilder confBuilder, int taskId)
        {
        }

        /// <summary>
        /// Utility method for logging the topology state.
        /// This will be called every time a topology object is built or modified
        /// because of a failure.
        /// </summary>
        public string LogTopologyState()
        {
            return "empty";
        }

        /// <summary>
        /// This method is triggered when a node detects a change in the topology and asks the driver for an update.
        /// </summary>
        /// <param name="taskId">The identifier of the task asking for the update</param>
        /// <param name="returnMessages">A list of message containing the topology update</param>
        /// <param name="failureStateMachine">An optional failure machine to log updates</param>
        public void TopologyUpdateResponse(string taskId, ref List<IElasticDriverMessage> returnMessages, Optional<IFailureStateMachine> failureStateMachine)
        {
        }

        /// <summary>
        /// Action to trigger when the operator recdeives a notification that a new iteration is started.
        /// This method does nothing.
        /// </summary>
        /// <param name="iteration">The new iteration number</param>
        public void OnNewIteration(int iteration)
        {
        }

        /// <summary>
        /// Reconfigure the topology in response to some event.
        /// </summary>
        /// <param name="taskId">The task id responsible for the topology change</param>
        /// <param name="info">Some additional topology-specific information</param>
        /// <param name="iteration">The optional iteration number in which the event occurred</param>
        /// <returns>An empty list of messages</returns>
        public IList<IElasticDriverMessage> Reconfigure(string taskId, Optional<string> info, Optional<int> iteration)
        {
            return new List<IElasticDriverMessage>();
        }

        /// <summary>
        /// Log the final statistics of the operator.
        /// This is called when the pipeline execution is completed.
        /// </summary>
        public string LogFinalStatistics()
        {
            return string.Empty;
        }
    }
}
