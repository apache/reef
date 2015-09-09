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
using System.Linq;
using System.Text;
using System.Threading;
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
        private const int NumberOfTasksToSubmitOnRestart = 1;

        private readonly IEvaluatorRequestor _evaluatorRequestor;

        private bool _isRestart;
        private readonly ISet<string> _newlyReceivedEvaluators = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly ISet<string> _expectToRecoverEvaluators = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly ISet<string> _expiredEvaluators = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly ISet<string> _recoveredEvaluators = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly object _lockObj = new object();
        private readonly Timer _exceptionTimer;
        
        private int _newRunningTaskCount = 0;
        private int _finishedRecoveredTaskCount = 0;
        private int _finishedNewRunningTaskCount = 0;

        [Inject]
        private HelloRestartDriver(IEvaluatorRequestor evaluatorRequestor)
        {
            _exceptionTimer = new Timer(obj =>
            {
                throw new Exception("Expected driver to be finished by now.");
            }, new object(), TimeSpan.FromMinutes(10), TimeSpan.FromMinutes(10));

            _evaluatorRequestor = evaluatorRequestor;
        }

        /// <summary>
        /// Submits the HelloRestartTask to the Evaluator.
        /// </summary>
        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            lock (_lockObj)
            {
                _newlyReceivedEvaluators.Add(allocatedEvaluator.Id);
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
            _isRestart = false;
            Logger.Log(Level.Info, "HelloRestartDriver started at {0}", driverStarted.StartTime);
            _evaluatorRequestor.Submit(_evaluatorRequestor.NewBuilder().SetNumber(NumberOfTasksToSubmit).SetMegabytes(64).Build());
        }

        /// <summary>
        /// Prints a restart message and enters the restart codepath.
        /// </summary>
        public void OnNext(IDriverRestarted value)
        {
            _isRestart = true;
            Logger.Log(Level.Info, "Hello! HelloRestartDriver has restarted! Expecting these Evaluator IDs [{0}]", string.Join(", ", value.ExpectedEvaluatorIds));
            _expectToRecoverEvaluators.UnionWith(value.ExpectedEvaluatorIds);

            Logger.Log(Level.Info, "Requesting {0} new Evaluators on restart.", NumberOfTasksToSubmitOnRestart);
            _evaluatorRequestor.Submit(_evaluatorRequestor.NewBuilder().SetNumber(NumberOfTasksToSubmitOnRestart).SetMegabytes(64).Build());
        }

        public void OnNext(IActiveContext value)
        {
            Logger.Log(Level.Info, "{0} active context {1} from evaluator with ID [{2}].", GetReceivedOrRecovered(value.EvaluatorId), value.Id, value.EvaluatorId);
        }

        public void OnNext(IRunningTask value)
        {
            lock (_lockObj)
            {
                Logger.Log(Level.Info, "{0} running task with ID [{1}] from evaluator with ID [{2}]", 
                    GetReceivedOrRecovered(value.ActiveContext.EvaluatorId), value.Id, value.ActiveContext.EvaluatorId);

                if (IsExpectToRecoverEvaluator(value.ActiveContext.EvaluatorId))
                {
                    value.Send(Encoding.UTF8.GetBytes("Hello from driver!"));
                    _recoveredEvaluators.Add(value.ActiveContext.EvaluatorId);
                }
                else
                {
                    _newRunningTaskCount++;

                    // Kill itself in order for the driver to restart it.
                    if (!_isRestart && _newRunningTaskCount == NumberOfTasksToSubmit)
                    {
                        Process.GetCurrentProcess().Kill();
                    }
                    else if (_isRestart)
                    {
                        value.Send(Encoding.UTF8.GetBytes("Hello from driver!"));
                        if (_newRunningTaskCount == NumberOfTasksToSubmitOnRestart)
                        {
                            Logger.Log(Level.Info, "Received all requested new running tasks.");
                        }
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
            bool expired;
            lock (_lockObj)
            {
                restart = !_newlyReceivedEvaluators.Contains(value.Id);
                expired = IsExpectToRecoverEvaluator(value.Id);
                
                // _recoveredEvaluators is checked due to a race condition in REEF-61.
                // TODO [REEF-61]: Fix this check.
                if (expired && !_recoveredEvaluators.Contains(value.Id))
                {
                    _expiredEvaluators.Add(value.Id);
                }
            }

            string action;
            if (restart)
            {
                action = expired ? "Expired on restart " : "Restart initialization ";
            }
            else
            {
                action = string.Empty;
            }

            Logger.Log(Level.Info, action + "Evaluator [" + value + "] has failed!");

            CheckSuccess();
        }

        public void OnError(Exception error)
        {
            _exceptionTimer.Dispose();
            throw error;
        }

        public void OnCompleted()
        {
            _exceptionTimer.Dispose();
        }

        private void IncrementFinishedTask(Optional<IActiveContext> activeContext)
        {
            lock (_lockObj)
            {
                if (activeContext.IsPresent())
                {
                    if (_recoveredEvaluators.Contains(activeContext.Value.EvaluatorId))
                    {
                        Logger.Log(Level.Info, "Task on recovered Evaluator [{0}] has finished.",
                            activeContext.Value.EvaluatorId);
                        _finishedRecoveredTaskCount++;
                        if (_finishedRecoveredTaskCount == _expectToRecoverEvaluators.Count)
                        {
                            Logger.Log(Level.Info, "All recovered tasks have finished!");
                        }
                    }
                    else
                    {
                        Logger.Log(Level.Info, "Newly allocated task on Evaluator [{0}] has finished.",
                            activeContext.Value.EvaluatorId);

                        if (_isRestart)
                        {
                            _finishedNewRunningTaskCount++;
                            if (_finishedNewRunningTaskCount == NumberOfTasksToSubmitOnRestart)
                            {
                                Logger.Log(Level.Info, "All newly submitted tasks have finished.");
                            }
                        }
                    }

                    activeContext.Value.Dispose();

                    CheckSuccess();
                }
                else
                {
                    throw new Exception("Active context is expected to be present.");
                }
            }
        }

        private void CheckSuccess()
        {
            lock (_lockObj)
            {
                var recoverySuccess = _expectToRecoverEvaluators.SetEquals(_expiredEvaluators.Concat(_recoveredEvaluators));
                var taskRecoverSuccess = _finishedRecoveredTaskCount == _recoveredEvaluators.Count;
                var newEvalSuccess = _finishedNewRunningTaskCount == NumberOfTasksToSubmitOnRestart;

                if (recoverySuccess && newEvalSuccess && taskRecoverSuccess)
                {
                    Logger.Log(Level.Info, "SUCCESS");
                }
            }
        }

        private bool IsExpectToRecoverEvaluator(string evaluatorId)
        {
            return _expectToRecoverEvaluators.Contains(evaluatorId);
        }

        private string GetReceivedOrRecovered(string evaluatorId)
        {
            return IsExpectToRecoverEvaluator(evaluatorId) ? "Recovered" : "Received";
        }
    }
}