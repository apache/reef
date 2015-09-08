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
using System.Diagnostics;
using System.Text;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;
using IRunningTask = Org.Apache.REEF.Driver.Task.IRunningTask;

namespace Org.Apache.REEF.Examples.DriverRestart
{
    /// <summary>
    /// The Driver for HelloRestartREEF.
    /// This driver is meant to run on YARN on HDInsight, with the ability to keep containers
    /// across application attempts.
    /// It requests 1 evaluators and runs a running task on each of the evaluators.
    /// Once all tasks are running, the driver kills itself and expects the RM to restart it.
    /// On restart, it expects all of the running task(s) to report back to it.
    /// </summary>
    public sealed class HelloRestartDriver : IObserver<IDriverRestartCompleted>, IObserver<IAllocatedEvaluator>, IObserver<IDriverStarted>, 
        IObserver<IDriverRestarted>, IObserver<IActiveContext>, IObserver<IRunningTask>, IObserver<ICompletedTask>, IObserver<IFailedTask>,
        IObserver<IFailedEvaluator>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(HelloRestartDriver));
        private const int NumberOfTasksToSubmit = 1;

        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private readonly ISet<string> _receivedEvaluators = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly object _lockObj;
        
        private int _runningTaskCount;
        private int _finishedTaskCount;
        private bool _restarted;

        [Inject]
        private HelloRestartDriver(IEvaluatorRequestor evaluatorRequestor)
        {
            _finishedTaskCount = 0;
            _runningTaskCount = 0;
            _evaluatorRequestor = evaluatorRequestor;
            _restarted = false;
            _lockObj = new object();
        }

        /// <summary>
        /// Submits the HelloRestartTask to the Evaluator.
        /// </summary>
        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            lock (_lockObj)
            {
                _receivedEvaluators.Add(allocatedEvaluator.Id);
            }

            var taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "HelloRestartTask")
                .Set(TaskConfiguration.Task, GenericType<HelloRestartTask>.Class)
                .Set(TaskConfiguration.OnMessage, GenericType<HelloRestartTask>.Class)
                .Build();

            allocatedEvaluator.SubmitTask(taskConfiguration);
        }

        /// <summary>
        /// Called to start the driver.
        /// </summary>
        public void OnNext(IDriverStarted driverStarted)
        {
            Logger.Log(Level.Info, "HelloRestartDriver started at {0}", driverStarted.StartTime);
            _evaluatorRequestor.Submit(_evaluatorRequestor.NewBuilder().SetNumber(NumberOfTasksToSubmit).SetMegabytes(64).Build());
        }

        /// <summary>
        /// Prints a restart message and enters the restart codepath.
        /// </summary>
        public void OnNext(IDriverRestarted value)
        {
            _restarted = true;
            Logger.Log(Level.Info, "Hello! HelloRestartDriver has restarted! Expecting these Evaluator IDs [{0}]", string.Join(", ", value.ExpectedEvaluatorIds));
        }

        public void OnNext(IActiveContext value)
        {
            Logger.Log(Level.Info, "Received active context {0} from evaluator with ID [{1}].", value.Id, value.EvaluatorId);
        }

        public void OnNext(IRunningTask value)
        {
            lock (_lockObj)
            {
                _runningTaskCount++;
                
                Logger.Log(Level.Info, "Received running task with ID [{0}] " +
                              " with restart set to {1} from evaluator with ID [{2}]",
                              value.Id, _restarted, value.ActiveContext.EvaluatorId);

                if (_restarted)
                {
                    value.Send(Encoding.UTF8.GetBytes("Hello from driver!"));
                }

                // Kill itself in order for the driver to restart it.
                if (_runningTaskCount == NumberOfTasksToSubmit)
                {
                    if (_restarted)
                    {
                        Logger.Log(Level.Info, "Retrieved all running tasks from the previous instance.");
                    }
                    else
                    {
                        Process.GetCurrentProcess().Kill();
                    }
                }
            }
        }

        public void OnNext(IDriverRestartCompleted value)
        {
            var timedOutStr = (value.IsTimedOut ? " due to timeout" : string.Empty);
            Logger.Log(Level.Info, "Driver restart has completed" + timedOutStr + ".");
        }

        public void OnNext(ICompletedTask value)
        {
            IncrementFinishedTask(Optional<IActiveContext>.Of(value.ActiveContext));
        }

        public void OnNext(IFailedTask value)
        {
            IncrementFinishedTask(value.GetActiveContext());
        }

        public void OnNext(IFailedEvaluator value)
        {
            bool restart;
            lock (_lockObj)
            {
                restart = !this._receivedEvaluators.Contains(value.Id);
            }

            var append = restart ? "Restarted recovered " : string.Empty;

            Logger.Log(Level.Info, append + "Evaluator [" + value + "] has failed!");
        }

        public void OnError(Exception error)
        {
            throw error;
        }

        public void OnCompleted()
        {
        }

        private void IncrementFinishedTask(Optional<IActiveContext> activeContext)
        {
            lock (_lockObj)
            {
                _finishedTaskCount++;
                if (_finishedTaskCount == NumberOfTasksToSubmit)
                {
                    Logger.Log(Level.Info, "All tasks are done! Driver should exit now...");
                }

                if (activeContext.IsPresent())
                {
                    activeContext.Value.Dispose();
                }
            }
        }
    }
}