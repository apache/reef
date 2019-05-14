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

using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Attributes;
using System;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Driver
{
    /// <summary>
    /// Class defining how groups of tasks sharing similar scheduling semantics are managed.
    /// Task set managers subscribe to stages in order to define tasks logic.
    /// Task set managers schedule and manage group of tasks running in the cluster.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public interface IElasticTaskSetManager : IFailureResponse, IDisposable
    {
        /// <summary>
        /// An identifier for the set of Stages the Task Manager is subscribed to.
        /// The task set has to be built before retrieving its stages id.
        /// </summary>
        string StagesId { get; }

        /// <summary>
        /// Decides whether more contexts have to be added to this Task Manger or not.
        /// </summary>
        /// <returns>True if the number of added contexts is less than the available slots</returns>
        bool HasMoreContextToAdd { get; }

        /// <summary>
        /// Whether this task set manger is done.
        /// </summary>
        bool IsCompleted { get; }

        /// <summary>
        /// Subscribe the current task set manager to a new stage.
        /// </summary>
        /// <param name="stage">The stage to subscribe to</param>
        /// <returns>The task manager with the added stage</returns>
        IElasticTaskSetManager AddStage(IElasticStage stage);

        /// <summary>
        /// Method used to generate unique context ids.
        /// </summary>
        /// <param name="evaluator">The evaluator the context will run on</param>
        /// <param name="identifier">A new unique context id</param>
        /// <returns>True if an new context id is sucessufully created</returns>
        bool TryGetNextTaskContextId(IAllocatedEvaluator evaluator, out string identifier);

        /// <summary>
        /// Method used to generate unique task ids.
        /// </summary>
        /// <param name="context">The context the task will run on</param>
        /// <returns>A new task id</returns>
        string GetTaskId(IActiveContext context);

        /// <summary>
        /// Finalizes the task set manager.
        /// After the task set has been finalized, no more stages can be added.
        /// </summary>
        /// <returns>The same finalized task set manager</returns>
        IElasticTaskSetManager Build();

        /// <summary>
        /// Retrieves all stages having the context passed as a parameter
        /// as master task context.
        /// </summary>
        /// <param name="context">The target context</param>
        /// <returns>A list of stages having the master task running on context</returns>
        IEnumerable<IElasticStage> IsMasterTaskContext(IActiveContext context);

        /// <summary>
        /// Get the configuration of the codecs used for data transmission.
        /// The codecs are automatically generated from the operator pipeline.
        /// </summary>
        /// <returns>A configuration object with the codecs for data transmission</returns>
        IConfiguration GetCodecConfiguration();

        /// <summary>
        /// Method implementing how the task set manager should react when a new context is active.
        /// </summary>
        /// <param name="activeContext">The new active context</param>
        void OnNewActiveContext(IActiveContext activeContext);

        /// <summary>
        /// Method implementing how the task set manager should react when a notification that a task is running is received.
        /// </summary>
        /// <param name="task">The running task</param>
        void OnTaskRunning(IRunningTask task);

        /// <summary>
        /// Method implementing how the task set manager should react when a notification that a task is completed is received.
        /// </summary>
        /// <param name="task">The completed task</param>
        void OnTaskCompleted(ICompletedTask task);

        /// <summary>
        /// Method implementing how the task set manager should react when a task message is received.
        /// </summary>
        /// <param name="task">A message from a task</param>
        void OnTaskMessage(ITaskMessage message);

        /// <summary>
        /// Whether the imput task is managed by this task set manger.
        /// </summary>
        /// <param name="id">The task identifier</param>
        bool IsTaskManagedBy(string id);

        /// <summary>
        /// Whether the imput context is managed by this task set manger.
        /// </summary>
        /// <param name="id">The context identifier</param>
        bool IsContextManagedBy(string id);

        /// <summary>
        /// Whether the imput evaluator is managed by this task set manger.
        /// </summary>
        /// <param name="id">The context identifier</param>
        bool IsEvaluatorManagedBy(string id);

        /// <summary>
        /// Used to react on a task failure.
        /// </summary>
        /// <param name="task">The failed task</param>
        void OnTaskFailure(IFailedTask task);

        /// <summary>
        /// Used to react of a failure event occurred on an evaluator.
        /// </summary>
        /// <param name="evaluator">The failed evaluator</param>
        void OnEvaluatorFailure(IFailedEvaluator evaluator);
    }
}
