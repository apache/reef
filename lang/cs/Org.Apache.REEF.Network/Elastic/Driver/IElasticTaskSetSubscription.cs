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
using Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Network.Elastic.Comm;

namespace Org.Apache.REEF.Network.Elastic.Driver
{
    /// <summary>
    /// Used to group elastic operators into logical units. 
    /// All operators in the same subscriptions share similar semantics and behavior 
    /// under failures. Subscriptions can only be created by a service.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public interface IElasticTaskSetSubscription : IFailureResponse, ITaskMessageResponse
    {
        /// <summary>
        /// The name of the subscriptions.
        /// </summary>
        string SubscriptionName { get; }

        /// <summary>
        /// The operator at the beginning of the computation workflow.
        /// </summary>
        ElasticOperator RootOperator { get; }

        /// <summary>
        /// The failure state of the target subscriptions. 
        /// </summary>
        IFailureState FailureState { get; }

        /// <summary>
        /// The service managing the subscriptions.
        /// </summary>
        IElasticTaskSetService Service { get; }

        /// <summary>
        /// Whether the subscriptions is completed or not.
        /// </summary>
        bool IsCompleted { get; set; }

        /// <summary>
        /// Whether the subscriptions contains iterations or not.
        /// </summary>
        bool IsIterative { get; set; }

        /// <summary>
        /// Generates an id to uniquely identify Operators in the subscriptions.
        /// </summary>
        /// <returns>A new unique id</returns>
        int GetNextOperatorId();

        /// <summary>
        /// Add a partitioned dataset to the subscription.
        /// </summary>
        /// <param name="inputDataSet">The partitioned dataset</param>
        /// <param name="isMasterGettingInputData">Whether the master node should get a partition</param>
        void AddDataset(IPartitionedInputDataSet inputDataSet, bool isMasterGettingInputData = false);

        /// <summary>
        /// Add a set of datasets to the subscription.
        /// </summary>
        /// <param name="inputDataSet">The configuration for the datasets</param>
        /// <param name="isMasterGettingInputData">Whether the master node should get a partition</param>
        void AddDataset(IConfiguration[] inputDataSet, bool isMasterGettingInputData = false);

        /// <summary>
        /// Finalizes the subscriptions.
        /// After the subscriptions has been finalized, no more operators can
        /// be added to the group.
        /// </summary>
        /// <returns>The same finalized subscriptions</returns>
        IElasticTaskSetSubscription Build();

        /// <summary>
        /// Add a task to the subscriptions.
        /// The subscriptions must have been buit before tasks can be added.
        /// </summary>
        /// <param name="taskId">The id of the task to add</param>
        /// <returns>True if the task is correctly added to the subscriptions</returns>
        bool AddTask(string taskId);

        /// <summary>
        /// Decides if the tasks added to the subscriptions can be scheduled for execution
        /// or not. This method is used for implementing different policies for 
        /// triggering the scheduling of tasks.
        /// </summary>
        /// <returns>True if the previously added tasks can be scheduled for execution</returns>
        bool ScheduleSubscription();

        /// <summary>
        /// Whether the input activeContext is the one of the master tasks.
        /// </summary>
        /// <param name="activeContext">The active context of the task</param>
        /// <returns>True if the input parameter is the master task's active context</returns>
        bool IsMasterTaskContext(IActiveContext activeContext);

        /// <summary>
        /// Creates the Configuration for the input task.
        /// Must be called only after all tasks have been added to the subscriptions.
        /// </summary>
        /// <param name="builder">The configuration builder the configuration will be appended to</param>
        /// <param name="taskId">The task id of the task that belongs to this subscriptions</param>
        /// <returns>The configuration for the Task with added subscriptions informations</returns>
        IConfiguration GetTaskConfiguration(ref ICsConfigurationBuilder builder, int taskId);

        /// <summary>
        /// Given a task id, this method returns the configuration of the task's data partition
        /// (if any).
        /// </summary>
        /// <param name="taskId">The task id of the task we wanto to retrieve the data partition. 
        /// The task is required to belong to thq subscriptions</param>
        /// <returns>The configuration of the data partition (if any) of the task</returns>
        Optional<IConfiguration> GetPartitionConf(string taskId);

        /// <summary>
        /// Retrieve the log the final statistics of the computation: this is the sum of all 
        /// the stats of all the Operators compising the subscription. This method can be called
        /// only once the subscriptions is completed.
        /// </summary>
        /// <returns>The final statistics for the computation</returns>
        string LogFinalStatistics();
    }
}