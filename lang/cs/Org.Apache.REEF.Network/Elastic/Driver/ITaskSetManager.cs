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
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Utilities.Attributes;
using System;

namespace Org.Apache.REEF.Network.Elastic.Driver
{
    /// <summary>
    /// Class defining how groups of tasks sharing similar scheduling semantics are managed.
    /// TaskSets subscribe to Subscriptions in order to define tasks logic.
    /// TaskSets schedule and manage group of tasks running in the cluster.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public interface ITaskSetManager : IFailureResponse, IDisposable
    {
        /// <summary>
        /// An identifier for the set of Subscriptions the Task Manager is subscribed to.
        /// The Task Set has to be built before retrieving its subscriptions id.
        /// </summary>
        string SubscriptionsId { get; }

        /// <summary>
        /// Subscribe the current Task Set to a new Subscription.
        /// </summary>
        /// <param name="subscription">The subscription to subscribe to</param>
        void AddTaskSetSubscription(IElasticTaskSetSubscription subscription);

        /// <summary>
        /// Decides whether more contexts have to be added to this Task Manger or not.
        /// </summary>
        /// <returns>True if the number of added contexts is less than the available slots</returns>
        bool HasMoreContextToAdd();

        /// <summary>
        /// Method used to generate unique context ids.
        /// </summary>
        /// <param name="evaluator">The evaluator the context will run on</param>
        /// <returns>A new unique context id</returns>
        string GetNextTaskContextId(IAllocatedEvaluator evaluator);

        /// <summary>
        /// Method used to generate unique task ids.
        /// </summary>
        /// <param name="context">The context the task will run on</param>
        /// <returns>A new task id</returns>
        string GetNextTaskId(IActiveContext context);

        /// <summary>
        /// Finalizes the Task Set.
        /// After the Task set has been finalized, no more Subscriptions can be added.
        /// </summary>
        /// <returns>The same finalized Task Set</returns>
        ITaskSetManager Build();

        /// <summary>
        /// Retrieves all Subscriptions having the context passed as a parameter
        /// as master task context.
        /// </summary>
        /// <param name="context">The target context</param>
        /// <returns>A list of Subscriptions having the master task running on context</returns>
        IEnumerable<IElasticTaskSetSubscription> IsMasterTaskContext(IActiveContext context);

        /// <summary>
        /// Add a task to the Task Set.
        /// The Task Set must have called Build() before adding tasks.
        /// </summary>
        /// <param name="taskId">The id of the task to add</param>
        /// <param name="taskConfig">The current configuration of the task</param>
        /// <param name="context">The context the task will run on</param>
        void AddTask(string taskId, IConfiguration taskConfig, IActiveContext context);

        /// <summary>
        /// Actions to execute when a notification that a task is running is received.
        /// </summary>
        /// <param name="task">The running task</param>
        void OnTaskRunning(IRunningTask task);

        /// <summary>
        /// Actions to execute when a notification that a task is completed is received.
        /// </summary>
        /// <param name="task">The completed task</param>
        void OnTaskCompleted(ICompletedTask task);

        /// <summary>
        /// Actions to execute when a task message is received.
        /// </summary>
        /// <param name="task">A message from a task</param>
        void OnTaskMessage(ITaskMessage message);

        /// <summary>
        /// This method contains the logic to trigger when the Task Set execution is completed
        /// </summary>
        bool Done();

        /// <summary>
        /// Used to react of a failure of a task.
        /// </summary>
        /// <param name="evaluator">The failed task</param>
        void OnTaskFailure(IFailedTask info);

        /// <summary>
        /// Used to react of a failure event occurred on an evaluator.
        /// </summary>
        /// <param name="evaluator">The failed evaluator</param>
        void OnEvaluatorFailure(IFailedEvaluator evaluator);

        /// <summary>
        /// Contains the logic to trigger when the execution fails.
        /// </summary>
        /// <param name="taskId">The id of the task triggering the fail</param>
        void OnFail(string taskId);
    }
}