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
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl;

namespace Org.Apache.REEF.Network.Elastic.Driver
{
    /// <summary>
    /// Used to group operators in logical units.
    /// All operators in the same Subscription share similar semantics
    /// and behaviour under failures.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public interface IElasticTaskSetSubscription : IFailureResponse
    {
        /// <summary>
        /// The name of the Subscription.
        /// </summary>
        string SubscriptionName { get; }

        /// <summary>
        /// The operator at the beginning of the computation workflow.
        /// </summary>
        ElasticOperator RootOperator { get; }

        /// <summary>
        /// The Failure State of the target Subscription. 
        /// </summary>
        IFailureState FailureStatus { get; }

        /// <summary>
        /// The Service managing the Subscription.
        /// </summary>
        IElasticTaskSetService Service { get; }

        /// <summary>
        /// Generates an id to uniquely identify operators in the Subscription.
        /// </summary>
        /// <returns>A new unique id</returns>
        int GetNextOperatorId();

        /// <summary>
        /// Finalizes the Subscription.
        /// After the Subscription has been finalized, no more operators may
        /// be added to the group.
        /// </summary>
        /// <returns>The same finalized Subscription</returns>
        IElasticTaskSetSubscription Build();

        /// <summary>
        /// Add a task to the Subscription.
        /// The Subscription must have called Build() before adding tasks.
        /// </summary>
        /// <param name="taskId">The id of the task to add</param>
        /// <returns>True if the task is added to the Subscription</returns>
        bool AddTask(string taskId);

        /// <summary>
        /// Decides if the tasks added to the Subscription can be scheduled for execution
        /// or not. Method used for implementing different policies for 
        /// triggering the scheduling of tasks.
        /// </summary>
        /// <returns>True if the added tasks can be scheduled for execution</returns>
        bool ScheduleSubscription();

        /// <summary>
        /// Whether the input activeContext is the one of the master Task.
        /// </summary>
        /// <param name="activeContext">The active context for the task</param>
        /// <returns>True if the parameter is the master task's active context</returns>
        bool IsMasterTaskContext(IActiveContext activeContext);

        /// <summary>
        /// Creates the Configuration for the input task.
        /// Must be called only after all tasks have been added to the Subscription.
        /// </summary>
        /// <param name="builder">The configuration builder the configuration will be appended to</param>
        /// <param name="taskId">The task id of the task that belongs to this Subscription</param>
        /// <returns>The configuration for the Task with added Subscription informations</returns>
        void GetTaskConfiguration(ref ICsConfigurationBuilder builder, int taskId);
    }
}