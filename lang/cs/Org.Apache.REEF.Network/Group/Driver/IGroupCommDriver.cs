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
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Network.Group.Driver
{
    /// <summary>
    /// Used to create Communication Groups for Group Communication Operators.
    /// Also manages configuration for Group Communication tasks/services.
    /// </summary>
    public interface IGroupCommDriver
    {
        /// <summary>
        /// Returns the identifier for the master task
        /// </summary>
        string MasterTaskId { get; }

        ICommunicationGroupDriver DefaultGroup { get; }

        /// <summary>
        /// Create a new CommunicationGroup with the given name and number of tasks/operators. 
        /// </summary>
        /// <param name="groupName">The new group name</param>
        /// <param name="numTasks">The number of tasks/operators in the group.</param>
        /// <returns>The new Communication Group</returns>
        ICommunicationGroupDriver NewCommunicationGroup(string groupName, int numTasks);

        /// <summary>
        /// remove a communication group
        /// </summary>
        /// <param name="groupName"></param>
        void RemoveCommunicationGroup(string groupName);

        /// <summary>
        /// Generates context configuration with a unique identifier.
        /// </summary>
        /// <returns>The configured context configuration</returns>
        IConfiguration GetContextConfiguration();

        /// <summary>
        /// Get the service configuration required for running Group Communication on Reef tasks.
        /// </summary>
        /// <returns>The service configuration for the Reef tasks</returns>
        IConfiguration GetServiceConfiguration();

        /// <summary>
        /// Checks whether this active context can be used to run the Master Task.
        /// </summary>
        /// <param name="activeContext">The active context to check</param>
        /// <returns>True if the active context can run the Master task,
        /// otherwise false.</returns>
        bool IsMasterTaskContext(IActiveContext activeContext);

        /// <summary>
        /// Checks whether this context configuration is used to configure the Master Task.
        /// </summary>
        /// <param name="contextConfiguration">The context configuration to check</param>
        /// <returns>True if the context configuration is used to configure the Master
        /// Task, otherwise false.</returns>
        bool IsMasterContextConfiguration(IConfiguration contextConfiguration);

        /// <summary>
        /// Gets the context number associated with the Active Context id.
        /// </summary>
        /// <param name="activeContext">The active context to check</param>
        /// <returns>The context number associated with the active context id</returns>
        int GetContextNum(IActiveContext activeContext);

        /// <summary>
        /// Get the configuration for a particular task.  
        ///
        /// The task may belong to many Communication Groups, so each one is serialized
        /// in the configuration as a SerializedGroupConfig.
        ///
        /// The user must merge their part of task configuration (task id, task class)
        /// with this returned Group Communication task configuration.
        /// </summary>
        /// <param name="taskId">The id of the task Configuration to generate</param>
        /// <returns>The Group Communication task configuration with communication group and
        /// operator configuration set.</returns>
        IConfiguration GetGroupCommTaskConfiguration(string taskId);
    }
}
