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

using Org.Apache.REEF.Network.Elastic.Driver.Default;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Attributes;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Driver
{
    /// <summary>
    /// Delegate used to generate the task configuration for the input task.
    /// </summary>
    /// <param name="taskId">The identifier for the task</param>
    /// <returns></returns>
    public delegate IConfiguration TaskConfigurator(string taskId);

    /// <summary>
    /// This is the entry point for enabling the Elastic Group Communication.
    /// The workflow is the following:
    /// (1) Create a context instance;
    /// (2) Use the context to create one or more stages;
    /// (3) Use the stage to create a pipeline of operators representing the 
    ///     communication pattern the tasks should implement;
    /// (4) Create one or more task set managers to manage the scheduling of the tasks;
    /// (5) Register stage to the manager to properly configure the task set.
    /// 
    /// This interface is mainly used to create elastic stages.
    /// Also manages configurations for elastic group communication operators/stages.
    /// </summary>
    [Unstable("0.16", "API may change")]
    [DefaultImplementation(typeof(DefaultElasticContext))]
    public interface IElasticContext : IFailureResponse
    {
        /// <summary>
        /// Creates a stage with the default settings. 
        /// The stage lifecicle is managed by the context.
        /// </summary>
        /// <returns>A new stage with default parameters</returns>
        IElasticStage DefaultStage();

        /// <summary>
        /// Creates a new stage.
        /// The stage lifecicle is managed by the context.
        /// </summary>
        /// <param name="stageName">The name of the stage</param>
        /// <param name="numTasks">The number of tasks required by the stage</param>
        /// <param name="failureMachine">An optional failure machine governing the stage</param>
        /// <returns>The new task Set subscrption</returns>
        IElasticStage CreateNewStage(string stageName, int numTasks, IFailureStateMachine failureMachine = null);

        /// <summary>
        /// Remove a stage from the context.
        /// </summary>
        /// <param name="stageName">The name of the stage to be removed</param>
        void RemoveElasticStage(string stageName);

        /// <summary>
        /// Generate the base configuration module for tasks. 
        /// This method can be used to generate configurations for the task set menager.
        /// </summary>
        /// <param name="taskId">The id of the task the configuration is generate for</param>
        /// <returns>The module with the service properly set up for the task</returns>
        ConfigurationModule GetTaskConfigurationModule(string taskId);

        /// <summary>
        /// Start the elastic group communicatio context.
        /// This will trigger requests for resources as specified by the parameters.
        /// </summary>
        void Start();

        /// <summary>
        /// Create a new task set manager.
        /// </summary>
        /// <param name="masterTaskConfiguration">The configuration for the master task</param>
        /// <param name="slaveTaskConfiguration">The configuration for the slave task</param>
        /// <returns>A new task set manager</returns>
        IElasticTaskSetManager CreateNewTaskSetManager(
            TaskConfigurator masterTaskConfiguration,
            TaskConfigurator slaveTaskConfiguration = null);

        /// <summary>
        /// Create a new task set manager.
        /// </summary>
        /// <param name="numOfTasks">The number of tasks the task set should manager</param>
        /// <param name="masterTaskConfiguration">The configuration for the master task</param>
        /// <param name="slaveTaskConfiguration">The configuration for the slave task</param>
        /// <returns>A new task set manager</returns>
        IElasticTaskSetManager CreateNewTaskSetManager(
            int numOfTasks,
            TaskConfigurator masterTaskConfiguration,
            TaskConfigurator slaveTaskConfiguration = null);

        /// <summary>
        /// Generate the elastic service configuration object.
        /// This method is used to properly configure task contexts with the elastic service.
        /// </summary>
        /// <returns>The ealstic service configuration</returns>
        IConfiguration GetElasticServiceConfiguration();

        #region Serialization Helpers
        /// <summary>
        /// Append a stage configuration to a configuration builder object.
        /// </summary>
        /// <param name="confBuilder">The configuration where the stage configuration will be appended to</param>
        /// <param name="stageConf">The stage configuration at hand</param>
        /// <returns>The configuration containing the serialized stage configuration</returns>
        void SerializeStageConfiguration(ref ICsConfigurationBuilder confBuilder, IConfiguration stageConf);

        /// <summary>
        /// Append an operator configuration to a configuration builder object.
        /// </summary>
        /// <param name="serializedOperatorsConfs">The list where the operator configuration will be appended to</param>
        /// <param name="stageConf">The operator configuration at hand</param>
        /// <returns>The configuration containing the serialized operator configuration</returns>
        void SerializeOperatorConfiguration(ref IList<string> serializedOperatorsConfs, IConfiguration operatorConfiguration);
        #endregion
    }
}
