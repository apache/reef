/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Org.Apache.Reef.Driver.Context;
using Org.Apache.Reef.IO.Network.Utilities;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Implementations.Configuration;
using Org.Apache.Reef.Tang.Interface;

namespace Org.Apache.Reef.IO.Network.Group.Driver.Impl
{
    /// <summary>
    /// Helper class to start MPI tasks.
    /// </summary>
    public class TaskStarter
    {
        private static Logger LOGGER = Logger.GetLogger(typeof(TaskStarter));

        private object _lock;
        private int _numTasks;
        private int _tasksAdded;
        private string _masterTaskId;

        private IMpiDriver _mpiDriver;
        private List<Tuple<string, IConfiguration, IActiveContext>> _taskTuples; 

        /// <summary>
        /// Create new TaskStarter.
        /// After adding the correct number of tasks to the TaskStarter, the
        /// Tasks will be started on their given active context.
        /// </summary>
        /// <param name="mpiDriver">The IMpiDriver for the MPI tasks</param>
        /// <param name="numTasks">The number of Tasks that need to be added before
        /// the Tasks will be started. </param>
        public TaskStarter(IMpiDriver mpiDriver, int numTasks)
        {
            LOGGER.Log(Level.Verbose, "Creating TaskStarter");
            _masterTaskId = mpiDriver.MasterTaskId;
            _numTasks = numTasks;
            _tasksAdded = 0;
            _lock = new object();

            _mpiDriver = mpiDriver;
            _taskTuples = new List<Tuple<string, IConfiguration, IActiveContext>>();
        }

        /// <summary>
        /// Queues the task into the TaskStarter.
        /// 
        /// Once the correct number of tasks have been queued, the final Configuration
        /// will be generated and run on the given Active Context.
        /// </summary>
        /// <param name="partialTaskConfig">The partial task configuration containing Task
        /// identifier and Task class</param>
        /// <param name="activeContext">The Active Context to run the Task on</param>
        public void QueueTask(IConfiguration partialTaskConfig, IActiveContext activeContext)
        {
            string taskId = Utils.GetTaskId(partialTaskConfig); 
            LOGGER.Log(Level.Verbose, "Adding context with identifier: " + taskId);
            
            lock (_lock)
            {
                _taskTuples.Add(
                    new Tuple<string, IConfiguration, IActiveContext>(taskId, partialTaskConfig, activeContext));

                if (Interlocked.Increment(ref _tasksAdded) == _numTasks)
                {
                    StartTasks();
                }
            }
        }

        /// <summary>
        /// Starts the Master Task followed by the Slave Tasks.
        /// </summary>
        private void StartTasks()
        {
            Tuple<string, IConfiguration, IActiveContext> masterTaskTuple;
            try
            {
                masterTaskTuple = _taskTuples.Single(tuple => tuple.Item1.Equals(_masterTaskId));
            }
            catch (InvalidOperationException)
            {
                LOGGER.Log(Level.Error, "There must be exactly one master task. The driver has been misconfigured.");
                throw;
            }

            LOGGER.Log(Level.Verbose, "Starting master task on context id: {0}.", masterTaskTuple.Item3.Id);
            StartTask(masterTaskTuple.Item1, masterTaskTuple.Item2, masterTaskTuple.Item3);

            LOGGER.Log(Level.Verbose, "Starting slave tasks.");
            foreach (Tuple<string, IConfiguration, IActiveContext> taskTuple in _taskTuples)
            {
                string taskId = taskTuple.Item1;
                if (taskId.Equals(_masterTaskId))
                {
                    continue;
                }

                StartTask(taskId, taskTuple.Item2, taskTuple.Item3);
            }
        }

        private void StartTask(
            string taskId,
            IConfiguration userPartialTaskConf,
            IActiveContext activeContext)
        {
            IConfiguration mpiTaskConfiguration = _mpiDriver.GetMpiTaskConfiguration(taskId);
            IConfiguration mergedTaskConf = Configurations.Merge(userPartialTaskConf, mpiTaskConfiguration);
            activeContext.SubmitTask(mergedTaskConf);
        }
    }
}
