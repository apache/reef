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
        private readonly IDictionary<string, EvaluatorState> _evaluators = new Dictionary<string, EvaluatorState>(StringComparer.OrdinalIgnoreCase);

        private readonly object _lockObj = new object();
        private readonly Timer _exceptionTimer;

        [Inject]
        private HelloRestartDriver(IEvaluatorRequestor evaluatorRequestor)
        {
            _exceptionTimer = new Timer(obj =>
            {
                throw new ApplicationException("Expected driver to be finished by now.");
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
                _evaluators.Add(allocatedEvaluator.Id, EvaluatorState.NewAllocated);
            }

            var taskConfiguration = TaskConfiguration.ConfigurationModule
                .Set(TaskConfiguration.Identifier, "HelloRestartTask")
                .Set(TaskConfiguration.Task, GenericType<HelloRestartTask>.Class)
                .Set(TaskConfiguration.OnMessage, GenericType<HelloRestartTask>.Class)
                .Set(TaskConfiguration.OnDriverConnectionChanged, GenericType<HelloRestartTask>.Class)
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
            if (value.ResubmissionAttempts != 1)
            {
                throw new ApplicationException("Only expected the driver to restart once.");
            }

            _isRestart = true;
            Logger.Log(Level.Info, "Hello! HelloRestartDriver has restarted! Expecting these Evaluator IDs [{0}]", string.Join(", ", value.ExpectedEvaluatorIds));
            foreach (var expectedEvaluatorId in value.ExpectedEvaluatorIds)
            {
                _evaluators.Add(expectedEvaluatorId, EvaluatorState.Expected);
            }

            Logger.Log(Level.Info, "Requesting {0} new Evaluators on restart.", NumberOfTasksToSubmitOnRestart);
            _evaluatorRequestor.Submit(_evaluatorRequestor.NewBuilder().SetNumber(NumberOfTasksToSubmitOnRestart).SetMegabytes(64).Build());
        }

        public void OnNext(IActiveContext value)
        {
            if (!_evaluators.ContainsKey(value.EvaluatorId))
            {
                throw new Exception("Received active context from unexpected Evaluator " + value.EvaluatorId);
            }

            Logger.Log(Level.Info, "{0} active context {1} from evaluator with ID [{2}].", _evaluators[value.EvaluatorId], value.Id, value.EvaluatorId);
        }

        public void OnNext(IRunningTask value)
        {
            lock (_lockObj)
            {
                var evaluatorId = value.ActiveContext.EvaluatorId;
                if (!_evaluators.ContainsKey(evaluatorId))
                {
                    throw new Exception("Unexpected Running Task from Evaluator " + evaluatorId);
                }

                Logger.Log(Level.Info, "{0} running task with ID [{1}] from evaluator with ID [{2}]",
                    _evaluators[evaluatorId], value.Id, evaluatorId);

                if (_evaluators[evaluatorId] == EvaluatorState.Expected)
                {
                    value.Send(Encoding.UTF8.GetBytes("Hello from driver!"));
                    _evaluators[evaluatorId] = EvaluatorState.RecoveredRunning;
                }
                else if (_evaluators[value.ActiveContext.EvaluatorId] == EvaluatorState.NewAllocated)
                {
                    _evaluators[evaluatorId] = EvaluatorState.NewRunning;

                    var newRunningCount = CountState(EvaluatorState.NewRunning);

                    // Kill itself in order for the driver to restart it.
                    if (!_isRestart && newRunningCount == NumberOfTasksToSubmit)
                    {
                        Process.GetCurrentProcess().Kill();
                    }

                    if (_isRestart)
                    {
                        value.Send(Encoding.UTF8.GetBytes("Hello from driver!"));
                        if (newRunningCount == NumberOfTasksToSubmitOnRestart)
                        {
                            Logger.Log(Level.Info, "Received all requested new running tasks.");
                        }
                    }
                }
            }
        }

        public void OnNext(IDriverRestartCompleted value)
        {
            var timedOutStr = value.IsTimedOut ? " due to timeout" : string.Empty;
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
            string action;
            var evaluatorId = value.Id;
            lock (_lockObj)
            {
                if (!_evaluators.ContainsKey(evaluatorId))
                {
                    _evaluators[evaluatorId] = EvaluatorState.FailedAtRestartInit;
                    action = "Restart initialization ";
                }
                else
                {
                    var state = _evaluators[evaluatorId];
                    switch (state)
                    {
                        case EvaluatorState.Expected:
                            _evaluators[evaluatorId] = EvaluatorState.Expired;
                            action = "Expired on restart ";
                            break;
                        case EvaluatorState.RecoveredFinished:
                        case EvaluatorState.NewFinished:
                            // Note: this can be a result of REEF-61 as well, so we ignore Finished tasks and don't mark them as UnexpectedFailed.
                            action = "Finished (REEF-61) ";
                            break;
                        default:
                            _evaluators[evaluatorId] = EvaluatorState.UnexpectedFailed;
                            action = "Unexpectedly failed (with original state " + state + ") ";
                            break;
                    }
                }
            }

            Logger.Log(Level.Info, action + "Evaluator [" + evaluatorId + "] has failed!");

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
                    var evaluatorId = activeContext.Value.EvaluatorId;
                    if (!_evaluators.ContainsKey(evaluatorId))
                    {
                        throw new Exception("Unexpected finished/completed Task from Evaluator " + evaluatorId + ".");
                    }

                    if (_evaluators[evaluatorId] == EvaluatorState.RecoveredRunning)
                    {
                        Logger.Log(Level.Info, "Task on recovered Evaluator [{0}] has finished.", evaluatorId);
                        _evaluators[evaluatorId] = EvaluatorState.RecoveredFinished;
                    }
                    else
                    {
                        Logger.Log(Level.Info, "Newly allocated task on Evaluator [{0}] has finished.", evaluatorId);
                        _evaluators[evaluatorId] = EvaluatorState.NewFinished;

                        if (_isRestart)
                        {
                            if (CountState(EvaluatorState.NewFinished) == NumberOfTasksToSubmitOnRestart)
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
                if (CountState(EvaluatorState.Expected, EvaluatorState.NewRunning, EvaluatorState.RecoveredRunning,
                        EvaluatorState.NewAllocated) == 0 &&
                    _evaluators.Count == NumberOfTasksToSubmitOnRestart + NumberOfTasksToSubmit)
                {
                    var append = CountState(EvaluatorState.UnexpectedFailed) > 0 ? " However, there are evaluators that have unexpectedly failed " +
                        "in this trial. Please re-run or read through the logs to make sure that such evaluators are expected." : string.Empty;
                    Logger.Log(Level.Info, "SUCCESS!" + append);
                }
            }
        }

        private int CountState(params EvaluatorState[] states)
        {
            var set = new HashSet<EvaluatorState>(states);
            return _evaluators.Count(kv => set.Contains(kv.Value));
        }

        private enum EvaluatorState
        {
            NewAllocated,
            NewRunning,
            Expected,
            RecoveredRunning,
            NewFinished,
            RecoveredFinished,
            UnexpectedFailed,
            FailedAtRestartInit,
            Expired
        }
    }
}