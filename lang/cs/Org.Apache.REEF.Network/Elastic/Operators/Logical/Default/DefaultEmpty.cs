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

using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Attributes;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Default
{
    /// <summary>
    /// Empty operator implementing the default failure logic. To use only as root of pipelines.
    /// </summary>
    [Unstable("0.16", "API may change")]
    class DefaultEmpty : ElasticOperatorWithDefaultDispatcher
    {
        /// <summary>
        /// Basic constructor for the empty operator.
        /// </summary>
        /// <param name="stage">The stage the operator is part of</param>
        /// <param name="failureMachine">The failure machine goverining the opeartor</param>
        public DefaultEmpty(IElasticStage stage, IFailureStateMachine failureMachine) :
            base(stage, null, new EmptyTopology(), failureMachine)
        {
            OperatorName = Constants.Empty;
            MasterId = 1;
            WithinIteration = false;
        }

        public override void OnTaskFailure(IFailedTask task, ref List<IFailureEvent> failureEvents)
        {
            if (_next != null)
            {
                _next.OnTaskFailure(task, ref failureEvents);
            }
        }

        /// <summary>
        /// Logs the current operator state. 
        /// </summary>
        protected override void LogOperatorState()
        {
        }

        /// <summary>
        /// This method is operator specific and serializes the operator configuration into the input list.
        /// </summary>
        /// <param name="serializedOperatorsConfs">A list the serialized operator configuration will be appended to</param>
        /// <param name="taskId">The task id of the task that belongs to this operator</param>
        protected override void GetOperatorConfiguration(ref IList<string> serializedOperatorsConfs, int taskId)
        {
        }

        /// <summary>
        /// Binding from logical to physical operator. 
        /// </summary>
        /// <param name="builder">The configuration builder the binding will be added to</param>
        protected override void PhysicalOperatorConfiguration(ref ICsConfigurationBuilder confBuilder)
        {
        }

        /// <summary>
        /// Utility method gathering the set of master task ids of the operators in the current pipeline.
        /// </summary>
        /// <param name="masterTasks">The id of the master tasks of the current and successive operators</param>
        internal override void GatherMasterIds(ref HashSet<string> masterTasks)
        {
            if (!_operatorFinalized)
            {
                throw new IllegalStateException("Operator need to be build before finalizing the stage");
            }

            if (_next != null)
            {
                _next.GatherMasterIds(ref masterTasks);
            }
        }
    }
}
