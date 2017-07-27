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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Timers;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.Driver.StateMachine;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.IMRU.OnREEF.MapInputWithControlMessage;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.IMRU.OnREEF.ResultHandler;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Network.Group.Pipelining;
using Org.Apache.REEF.Network.Group.Pipelining.Impl;
using Org.Apache.REEF.Network.Group.Topology;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// Implements the IMRU driver on REEF with fault tolerant
    /// </summary>
    /// <typeparam name="TMapInput">Map Input</typeparam>
    /// <typeparam name="TMapOutput">Map output</typeparam>
    /// <typeparam name="TResult">Result</typeparam>
    /// <typeparam name="TPartitionType">Type of data partition (Generic type in IInputPartition)</typeparam>
    internal sealed class IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType> :
        IObserver<IDriverStarted>,
        IObserver<IAllocatedEvaluator>,
        IObserver<IActiveContext>,
        IObserver<ICompletedTask>,
        IObserver<IFailedEvaluator>,
        IObserver<IFailedContext>,
        IObserver<IFailedTask>,
        IObserver<IRunningTask>,
        IObserver<IEnumerable<IActiveContext>>,
        IObserver<IJobCancelled>
    {
        private static readonly Logger Logger =
            Logger.GetLogger(typeof(IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>));

        internal const string DoneActionPrefix = "DoneAction:";
        internal const string FailActionPrefix = "FailAction:";
        internal const string CompletedTaskMessage = "Received ICompletedTask";
        internal const string RunningTaskMessage = "Received IRunningTask";
        internal const string FailedTaskMessage = "Received IFailedTask";
        internal const string FailedEvaluatorMessage = "Received IFailedEvaluator";

        private readonly ConfigurationManager _configurationManager;
        private readonly int _totalMappers;
        private readonly IGroupCommDriver _groupCommDriver;
        private readonly INameServer _nameServer;
        private ConcurrentStack<IConfiguration> _perMapperConfigurationStack;
        private readonly ISet<IPerMapperConfigGenerator> _perMapperConfigs;
        private readonly bool _invokeGC;
        private readonly ServiceAndContextConfigurationProvider<TMapInput, TMapOutput, TPartitionType> _serviceAndContextConfigurationProvider;
        private IJobCancelled _cancelEvent;

        /// <summary>
        /// The lock for the driver. 
        /// </summary>
        private readonly object _lock = new object();

        /// <summary>
        /// Multiply this fact on average closing time to give room for tasks to be closed by itself.
        /// </summary>
        private const int TaskWaitingForCloseTimeFactor = 3;

        /// <summary>
        /// Manages Tasks, maintains task states and responsible for task submission for the driver.
        /// </summary>
        private TaskManager _taskManager;

        /// <summary>
        /// Manages Active Contexts for the driver.
        /// </summary>
        private readonly ActiveContextManager _contextManager;

        /// <summary>
        /// Manages allocated and failed Evaluators for driver.
        /// </summary>
        private readonly EvaluatorManager _evaluatorManager;

        /// <summary>
        /// Defines the max retry number for recoveries. It is configurable for the driver. 
        /// </summary>
        private readonly int _maxRetryNumberForFaultTolerant;

        /// <summary>
        /// System State of the driver. 
        /// <see href="https://issues.apache.org/jira/browse/REEF-1223"></see> 
        /// </summary>
        private SystemStateMachine _systemState;

        /// <summary>
        /// Shows if the driver is first try. Once the system enters recovery, it is set to false. 
        /// </summary>
        private bool _isFirstTry = true;

        /// <summary>
        /// It records the number of retry for the recoveries. 
        /// </summary>
        private int _numberOfRetries;

        /// <summary>
        /// Minimum timeout in milliseconds for TaskWaitingForClose
        /// </summary>
        private readonly int _minTaskWaitingForCloseTimeout;

        /// <summary>
        /// Manages lifecycle events for driver, like JobCancelled event.
        /// </summary>
        private readonly List<IDisposable> _disposableResources = new List<IDisposable>();

        /// <summary>
        /// An internal timer that monitors the timeout for driver events
        /// </summary>
        private Timer _timeoutMonitorTimer;

        /// <summary>
        /// Record evaluator ids that are closed after timeout.
        /// The CompletedTask and failedEvaluator events from those tasks should be ignored to avoid double counted.
        /// </summary>
        private readonly IList<string> _evaluatorsForceClosed = new List<string>();

        [Inject]
        private IMRUDriver(IPartitionedInputDataSet dataSet,
            [Parameter(typeof(PerMapConfigGeneratorSet))] ISet<IPerMapperConfigGenerator> perMapperConfigs,
            ConfigurationManager configurationManager,
            IEvaluatorRequestor evaluatorRequestor,
            [Parameter(typeof(CoresPerMapper))] int coresPerMapper,
            [Parameter(typeof(CoresForUpdateTask))] int coresForUpdateTask,
            [Parameter(typeof(MemoryPerMapper))] int memoryPerMapper,
            [Parameter(typeof(MemoryForUpdateTask))] int memoryForUpdateTask,
            [Parameter(typeof(AllowedFailedEvaluatorsFraction))] double failedEvaluatorsFraction,
            [Parameter(typeof(MaxRetryNumberInRecovery))] int maxRetryNumberInRecovery,
            [Parameter(typeof(MinTaskWaitingForCloseTimeout))] int minTaskWaitingForCloseTimeout,
            [Parameter(typeof(TimeoutMonitoringInterval))] int timeoutMonitoringInterval,
            [Parameter(typeof(InvokeGC))] bool invokeGC,
            IGroupCommDriver groupCommDriver,
            INameServer nameServer,
            IJobLifecycleManager lifecycleManager)
        {
            _configurationManager = configurationManager;
            _groupCommDriver = groupCommDriver;
            _nameServer = nameServer;
            _perMapperConfigs = perMapperConfigs;
            _totalMappers = dataSet.Count;
            _invokeGC = invokeGC;
            _maxRetryNumberForFaultTolerant = maxRetryNumberInRecovery;
            _minTaskWaitingForCloseTimeout = minTaskWaitingForCloseTimeout;

            _contextManager = new ActiveContextManager(_totalMappers + 1);
            _contextManager.Subscribe(this);

            var updateSpec = new EvaluatorSpecification(memoryForUpdateTask, coresForUpdateTask);
            var mapperSpec = new EvaluatorSpecification(memoryPerMapper, coresPerMapper);
            var allowedFailedEvaluators = (int)(failedEvaluatorsFraction * _totalMappers);
            _evaluatorManager = new EvaluatorManager(_totalMappers + 1, allowedFailedEvaluators, evaluatorRequestor, updateSpec, mapperSpec);

            _systemState = new SystemStateMachine();
            _serviceAndContextConfigurationProvider =
                new ServiceAndContextConfigurationProvider<TMapInput, TMapOutput, TPartitionType>(dataSet, configurationManager);

            if (lifecycleManager != null)
            {
                var handle = lifecycleManager.Subscribe(this as IObserver<IJobCancelled>);
                _disposableResources.Add(handle);
            }

            _timeoutMonitorTimer = new Timer();
            _timeoutMonitorTimer.Elapsed += TimeoutMonitor;
            _timeoutMonitorTimer.Interval = timeoutMonitoringInterval;
            if (timeoutMonitoringInterval > 0)
            {
                _timeoutMonitorTimer.Enabled = true;
            }

            var msg =
                string.Format(CultureInfo.InvariantCulture, "map task memory: {0}, update task memory: {1}, map task cores: {2}, update task cores: {3}, maxRetry: {4}, allowedFailedEvaluators: {5}, minTaskWaitingForCloseTimeout: {6}, timeoutMonitoringInterval: {7}.",
                    memoryPerMapper,
                    memoryForUpdateTask,
                    coresPerMapper,
                    coresForUpdateTask,
                    _maxRetryNumberForFaultTolerant,
                    allowedFailedEvaluators,
                    minTaskWaitingForCloseTimeout,
                    timeoutMonitoringInterval);
            Logger.Log(Level.Info, msg);
        }

        #region IDriverStarted
        /// <summary>
        /// Requests evaluators when driver starts
        /// </summary>
        /// <param name="value">Event fired when driver started</param>
        public void OnNext(IDriverStarted value)
        {
            //// TODO[REEF-598]: Set a timeout for this request to be satisfied. If it is not within that time, exit the Driver.
            _evaluatorManager.RequestUpdateEvaluator();
        }
        #endregion IDriverStarted

        #region IAllocatedEvaluator
        /// <summary>
        /// IAllocatedEvaluator handler. It will take the following action based on the system state:
        /// Case WaitingForEvaluator
        ///    Add Evaluator to the Evaluator Manager
        ///    submit Context and Services
        /// Case Fail
        ///    Do nothing. This is because the code that sets system Fail has executed FailedAction. It has shut down all the allocated evaluators/contexts. 
        ///    If a new IAllocatedEvaluator comes after it, we should not submit anything so that the evaluator is returned.
        /// Other cases - not expected
        /// </summary>
        /// <param name="allocatedEvaluator">The allocated evaluator</param>
        public void OnNext(IAllocatedEvaluator allocatedEvaluator)
        {
            Logger.Log(Level.Info, "AllocatedEvaluator memory [{0}], systemState {1}.", allocatedEvaluator.GetEvaluatorDescriptor().Memory, _systemState.CurrentState);
            lock (_lock)
            {
                using (Logger.LogFunction("IMRUDriver::IAllocatedEvaluator"))
                {
                    switch (_systemState.CurrentState)
                    {
                        case SystemState.WaitingForEvaluator:
                            if (!_evaluatorManager.IsMasterEvaluatorAllocated())
                            {
                                _evaluatorManager.AddMasterEvaluator(allocatedEvaluator);
                                _evaluatorManager.RequestMapEvaluators(_totalMappers);
                            }
                            else
                            {
                                _evaluatorManager.AddAllocatedEvaluator(allocatedEvaluator);
                            }
                            SubmitContextAndService(allocatedEvaluator);
                            break;
                        case SystemState.Fail:
                            Logger.Log(Level.Info,
                                "Receiving IAllocatedEvaluator event, but system is in FAIL state, ignore it.");
                            allocatedEvaluator.Dispose();
                            break;
                        default:
                            UnexpectedState(allocatedEvaluator.Id, "IAllocatedEvaluator");
                            break;
                    }
                }
            }
        }

        /// <summary>
        /// Gets context and service configuration for evaluator depending
        /// on whether it is for update/master function or for mapper function.
        /// Then submits Context and Service with the corresponding configuration
        /// </summary>
        /// <param name="allocatedEvaluator"></param>
        private void SubmitContextAndService(IAllocatedEvaluator allocatedEvaluator)
        {
            ContextAndServiceConfiguration configs;
            if (_evaluatorManager.IsMasterEvaluatorId(allocatedEvaluator.Id))
            {
                configs =
                    _serviceAndContextConfigurationProvider
                        .GetContextConfigurationForMasterEvaluatorById(
                            allocatedEvaluator.Id);
            }
            else
            {
                configs = _serviceAndContextConfigurationProvider
                    .GetDataLoadingConfigurationForEvaluatorById(
                        allocatedEvaluator.Id);
            }
            allocatedEvaluator.SubmitContextAndService(configs.Context, configs.Service);
        }
        #endregion IAllocatedEvaluator

        #region IActiveContext
        /// <summary>
        /// IActiveContext handler. It will take the following actions based on the system state:
        /// Case WaitingForEvaluator:
        ///    Adds Active Context to Active Context Manager
        /// Case Fail:
        ///    Closes the ActiveContext
        /// Other cases - not expected
        /// </summary>
        /// <param name="activeContext"></param>
        public void OnNext(IActiveContext activeContext)
        {
            Logger.Log(Level.Info, "Received Active Context {0}, systemState {1}.", activeContext.Id, _systemState.CurrentState);
            lock (_lock)
            {
                using (Logger.LogFunction("IMRUDriver::IActiveContext"))
                {
                    switch (_systemState.CurrentState)
                    {
                        case SystemState.WaitingForEvaluator:
                            _contextManager.Add(activeContext);
                            break;
                        case SystemState.Fail:
                            Logger.Log(Level.Info,
                                "Received IActiveContext event, but system is in FAIL state. Closing the context.");
                            activeContext.Dispose();
                            break;
                        default:
                            UnexpectedState(activeContext.Id, "IActiveContext");
                            break;
                    }
                }
            }
        }
        #endregion IActiveContext

        #region submit tasks
        /// <summary>
        /// Called from ActiveContextManager when all the expected active context are received.
        /// It changes the system state then calls SubmitTasks().
        /// </summary>
        /// <param name="value"></param>
        public void OnNext(IEnumerable<IActiveContext> value)
        {
            Logger.Log(Level.Info, "Received event from ActiveContextManager with NumberOfActiveContexts:" + (value != null ? value.Count() : 0));
            lock (_lock)
            {
                // When the event AllContextsAreReady happens, change the system state from WaitingForEvaluator to SubmittingTasks
                _systemState.MoveNext(SystemStateEvent.AllContextsAreReady);
                SubmitTasks(value);
            }
        }

        /// <summary>
        /// This method is responsible to prepare for the task submission then call SubmitTasks in TaskManager.
        /// It is called in both first time and recovery scenarios.
        /// Creates a new Communication Group and adds Group Communication Operators
        /// For each context, adds a task to the communication group.
        /// After all the tasks are added to the group, for each task, gets GroupCommTaskConfiguration from IGroupCommDriver 
        /// and merges it with the task configuration.
        /// When all the tasks are added, calls TaskManager to SubmitTasks().
        /// </summary>
        private void SubmitTasks(IEnumerable<IActiveContext> activeContexts)
        {
            Logger.Log(Level.Info, "SubmitTasks with system state : {0} at time: {1}.", _systemState.CurrentState, DateTime.Now);
            using (Logger.LogFunction("IMRUDriver::SubmitTasksConfiguration"))
            {
                if (!_isFirstTry)
                {
                    _groupCommDriver.RemoveCommunicationGroup(IMRUConstants.CommunicationGroupName);
                }

                UpdateMaterTaskId();
                _taskManager = new TaskManager(_totalMappers + 1, _groupCommDriver.MasterTaskId);
                var commGroup = AddCommunicationGroupWithOperators();
                _perMapperConfigurationStack = ConstructPerMapperConfigStack(_totalMappers);

                var taskIdAndContextMapping = new Dictionary<string, IActiveContext>();
                foreach (var activeContext in activeContexts)
                {
                    var taskId = _evaluatorManager.IsMasterEvaluatorId(activeContext.EvaluatorId)
                        ? _groupCommDriver.MasterTaskId
                        : GetMapperTaskIdByEvaluatorId(activeContext.EvaluatorId);
                    commGroup.AddTask(taskId);
                    taskIdAndContextMapping.Add(taskId, activeContext);
                    Logger.Log(Level.Info, "Adding {0} with associated context: {1} to communication group: {2}.", taskId, activeContext.Id, IMRUConstants.CommunicationGroupName);
                }

                foreach (var mapping in taskIdAndContextMapping)
                {
                    var taskConfig = _evaluatorManager.IsMasterEvaluatorId(mapping.Value.EvaluatorId)
                        ? GetMasterTaskConfiguration(mapping.Key)
                        : GetMapperTaskConfiguration(mapping.Value, mapping.Key);
                    var groupCommTaskConfiguration = _groupCommDriver.GetGroupCommTaskConfiguration(mapping.Key);
                    var mergedTaskConf = Configurations.Merge(taskConfig, groupCommTaskConfiguration);
                    _taskManager.AddTask(mapping.Key, mergedTaskConf, mapping.Value);
                }
            }
            _taskManager.SubmitTasks();
        }

        private void UpdateMaterTaskId()
        {
            if (_isFirstTry)
            {
                _groupCommDriver.MasterTaskId = _groupCommDriver.MasterTaskId + "-" + _numberOfRetries;
            }
            else
            {
                _groupCommDriver.MasterTaskId =
                    _groupCommDriver.MasterTaskId.Substring(0, _groupCommDriver.MasterTaskId.Length - 1) +
                    _numberOfRetries;
            }
        }
        #endregion submit tasks

        #region IRunningTask
        /// <summary>
        /// IRunningTask handler. The method is called when a task is running. The following action will be taken based on the system state:
        /// Case SubmittingTasks
        ///     Add it to RunningTasks and set task state to TaskRunning
        ///     When all the tasks are running, change system state to TasksRunning
        /// Case ShuttingDown/Fail
        ///     Call TaskManager to record RunningTask during SystemFailure
        /// Other cases - not expected 
        /// </summary>
        /// <param name="runningTask"></param>
        public void OnNext(IRunningTask runningTask)
        {
            Logger.Log(Level.Info, "{0} {1} from endpoint {2} at SystemState {3} retry # {4}.", RunningTaskMessage, runningTask.Id, GetEndPointFromTaskId(runningTask.Id), _systemState.CurrentState, _numberOfRetries);
            lock (_lock)
            {
                using (Logger.LogFunction("IMRUDriver::IRunningTask"))
                {
                    switch (_systemState.CurrentState)
                    {
                        case SystemState.SubmittingTasks:
                            _taskManager.RecordRunningTask(runningTask);
                            if (_taskManager.AreAllTasksRunning())
                            {
                                _systemState.MoveNext(SystemStateEvent.AllTasksAreRunning);
                                Logger.Log(Level.Info,
                                    "All tasks are running, SystemState {0}",
                                    _systemState.CurrentState);
                            }
                            break;
                        case SystemState.ShuttingDown:
                        case SystemState.Fail:
                            _taskManager.RecordRunningTaskDuringSystemFailure(runningTask, TaskManager.CloseTaskByDriver);
                            break;
                        default:
                            UnexpectedState(runningTask.Id, "IRunningTask");
                            break;
                    }
                }
            }
        }
        #endregion IRunningTask

        #region ICompletedTask
        /// <summary>
        /// ICompletedTask handler. It is called when a task is completed. The following action will be taken based on the System State:
        /// Case TasksRunning
        ///     Check if it is master task, then set master task completed    
        ///     Then record completed running and updates task state from TaskRunning to TaskCompleted
        ///     If all tasks are completed, sets system state to TasksCompleted and then go to Done action
        /// Case TasksCompleted:
        ///     Record, log and then ignore the event        
        /// Case ShuttingDown
        ///     Record completed running and updates task state to TaskCompleted
        ///     Try to recover
        /// Other cases - not expected 
        /// </summary>
        /// <param name="completedTask">The link to the completed task</param>
        public void OnNext(ICompletedTask completedTask)
        {
            Logger.Log(Level.Info, "{0} {1}, with systemState {2} in retry# {3}.", CompletedTaskMessage, completedTask.Id, _systemState.CurrentState, _numberOfRetries);
            
            lock (_lock)
            {
                if (_evaluatorsForceClosed.Contains(completedTask.ActiveContext.EvaluatorId))
                {
                    Logger.Log(Level.Info, "Evaluator {0} has been closed after task {1} timeout, ignoring ICompletedTask event.", completedTask.ActiveContext.EvaluatorId, completedTask.Id);
                    return;
                }
                switch (_systemState.CurrentState)
                {
                    case SystemState.TasksRunning:
                        _taskManager.RecordCompletedTask(completedTask);
                        if (_taskManager.IsJobDone())
                        {
                            _systemState.MoveNext(SystemStateEvent.AllTasksAreCompleted);
                            Logger.Log(Level.Info, "Master task is completed, systemState {0}", _systemState.CurrentState);
                            DoneAction();
                        }
                        break;

                    case SystemState.ShuttingDown:
                        // The task might be in running state or waiting for close, record the completed task
                        _taskManager.RecordCompletedTask(completedTask);
                        if (_taskManager.IsJobDone())
                        {
                            _systemState.MoveNext(SystemStateEvent.AllTasksAreCompleted);
                            Logger.Log(Level.Info, "Master task is completed, systemState {0}", _systemState.CurrentState);
                            DoneAction();
                        }
                        else
                        {
                            TryRecovery();
                        }
                        break;

                    case SystemState.TasksCompleted:
                        _taskManager.RecordCompletedTask(completedTask);
                        break;

                    default:
                        UnexpectedState(completedTask.Id, "ICompletedTask");
                        break;
                }
            }
        }
        #endregion ICompletedTask

        #region IFailedEvaluator
        /// <summary>
        /// IFailedEvaluator handler. It specifies what to do when an evaluator fails.
        /// Case WaitingForEvaluator
        ///     This happens in the middle of submitting contexts. We just need to remove the failed evaluator 
        ///     from EvaluatorManager and remove associated active context, if any, from ActiveContextManager
        ///     then checks if the system is recoverable. If yes, request another Evaluator 
        ///     If not recoverable, set system state to Fail then execute Fail action
        /// Case SubmittingTasks/TasksRunning
        ///     This happens either in the middle of Task submitting or all the tasks are running
        ///     Changes the system state to ShuttingDown
        ///     Removes Evaluator and associated context from EvaluatorManager and ActiveContextManager
        ///     Removes associated task from running task if it was running and change the task state to TaskFailedByEvaluatorFailure
        ///     Closes all the other running tasks
        ///     Try to recover in case it is the last failure received
        /// Case TasksCompleted:
        ///     Record, log and then ignore the failure. 
        /// Case ShuttingDown
        ///     This happens when we have received either FailedEvaluator or FailedTask, some tasks are running some are in closing.
        ///     Removes Evaluator and associated context from EvaluatorManager and ActiveContextManager
        ///     Removes associated task from running task if it was running, changes the task state to ClosedTask if it was waiting for close
        ///     otherwise changes the task state to FailedTaskEvaluatorError
        ///     Try to recover in case it is the last failure received
        /// Other cases - not expected 
        /// </summary>
        /// <param name="failedEvaluator"></param>
        public void OnNext(IFailedEvaluator failedEvaluator)
        {
            var endpoint = failedEvaluator.FailedTask.IsPresent()
               ? GetEndPoint(failedEvaluator.FailedTask.Value)
               : failedEvaluator.FailedContexts.Any()
                   ? GetEndPointFromContext(failedEvaluator.FailedContexts.First())
                   : "unknown_endpoint";

            Logger.Log(Level.Warning, "{0} {1} from endpoint {2} with systemState {3} in retry# {4} with Exception: {5}.", FailedEvaluatorMessage, failedEvaluator.Id, endpoint, _systemState.CurrentState, _numberOfRetries, failedEvaluator.EvaluatorException);

            lock (_lock)
            {
                using (Logger.LogFunction("IMRUDriver::IFailedEvaluator"))
                {
                    if (_evaluatorsForceClosed.Contains(failedEvaluator.Id))
                    {
                        Logger.Log(Level.Info, "Evaluator {0} has been closed after task {1} timeout, ignoring IFailedEvaluator event.", failedEvaluator.Id, failedEvaluator.FailedTask.IsPresent() ? failedEvaluator.FailedTask.Value.Id : "NoTaskId");
                        return;
                    }

                    var isMaster = _evaluatorManager.IsMasterEvaluatorId(failedEvaluator.Id);
                    _evaluatorManager.RecordFailedEvaluator(failedEvaluator.Id);
                    _contextManager.RemoveFailedContextInFailedEvaluator(failedEvaluator);

                    switch (_systemState.CurrentState)
                    {
                        case SystemState.WaitingForEvaluator:
                            if (!_evaluatorManager.ExceededMaximumNumberOfEvaluatorFailures() && !isMaster)
                            {
                                _serviceAndContextConfigurationProvider.RemoveEvaluatorIdFromPartitionIdProvider(
                                    failedEvaluator.Id);
                                Logger.Log(Level.Info, "Requesting mapper Evaluators.");
                                _evaluatorManager.RequestMapEvaluators(1);
                            }
                            else
                            {
                                var reason1 = _evaluatorManager.ExceededMaximumNumberOfEvaluatorFailures()
                                    ? "it exceeded MaximumNumberOfEvaluatorFailures, "
                                    : string.Empty;
                                var reason2 = isMaster ? "master evaluator failed, " : string.Empty;
                                Logger.Log(Level.Error, "The system is not recoverable because " + reason1 + reason2 + " changing the system state to Fail.");
                                _systemState.MoveNext(SystemStateEvent.NotRecoverable);
                                FailAction();
                            }
                            break;

                        case SystemState.SubmittingTasks:
                        case SystemState.TasksRunning:
                            // When the event FailedNode happens, change the system state to ShuttingDown
                            _systemState.MoveNext(SystemStateEvent.FailedNode);
                            _taskManager.RecordTaskFailWhenReceivingFailedEvaluator(failedEvaluator);
                            _taskManager.CloseAllRunningTasks(TaskManager.CloseTaskByDriver);

                            // Push evaluator id back to PartitionIdProvider if it is not master
                            if (!isMaster)
                            {
                                _serviceAndContextConfigurationProvider.RemoveEvaluatorIdFromPartitionIdProvider(
                                    failedEvaluator.Id);
                            }

                            TryRecovery();
                            break;

                        case SystemState.TasksCompleted:
                            _taskManager.RecordTaskFailWhenReceivingFailedEvaluator(failedEvaluator);
                            Logger.Log(Level.Info, "The Job has been completed. So ignoring the Evaluator {0} failure.", failedEvaluator.Id);
                            break;

                        case SystemState.ShuttingDown:
                            _taskManager.RecordTaskFailWhenReceivingFailedEvaluator(failedEvaluator);

                            // Push evaluator id back to PartitionIdProvider if it is not master
                            if (!isMaster)
                            {
                                _serviceAndContextConfigurationProvider.RemoveEvaluatorIdFromPartitionIdProvider(
                                    failedEvaluator.Id);
                            }
                            TryRecovery();
                            break;

                        case SystemState.Fail:
                            FailAction();
                            break;

                        default:
                            UnexpectedState(failedEvaluator.Id, "IFailedEvaluator");
                            break;
                    }
                }
            }
        }
        #endregion IFailedEvaluator

        #region IFailedContext
        /// <summary>
        /// IFailedContext handler. It specifies what to do if Failed Context is received.
        /// If we get all completed tasks then ignore the failure otherwise throw exception
        /// Fault tolerant would be similar to FailedEvaluator.
        /// </summary>
        /// <param name="failedContext"></param>
        public void OnNext(IFailedContext failedContext)
        {
            Logger.Log(Level.Warning, "Received IFailedContext with Id: {0} from endpoint {1} with systemState {2} in retry#: {3}.", failedContext.Id, GetEndPointFromContext(failedContext), _systemState.CurrentState, _numberOfRetries);
            lock (_lock)
            {
                using (Logger.LogFunction("IMRUDriver::IFailedContext"))
                {
                    switch (_systemState.CurrentState)
                    {
                        case SystemState.TasksCompleted:
                            Logger.Log(Level.Info, "The Job has been completed. So ignoring the Context {0} failure.", failedContext.Id);
                            break;
                        case SystemState.ShuttingDown:
                        case SystemState.Fail:
                            break;
                        default:
                            var msg = string.Format(CultureInfo.InvariantCulture, "Context with Id: {0} failed with Evaluator id: {1}", failedContext.Id, failedContext.EvaluatorId);
                            throw new NotImplementedException(msg);
                    }
                }
            }
        }
        #endregion IFailedContext

        #region IFailedTask
        /// <summary>
        /// Case SubmittingTasks/TasksRunning
        ///     This is the first failure received
        ///     Changes the system state to ShuttingDown
        ///     Record failed task in TaskManager
        ///     Closes all the other running tasks and set their state to TaskWaitingForClose
        ///     Try to recover
        /// Case TasksCompleted:
        ///     Record, log and then ignore the failure. 
        /// Case ShuttingDown
        ///     This happens when we have received either FailedEvaluator or FailedTask, some tasks are running some are in closing.
        ///     Record failed task in TaskManager.
        ///     Try to recover
        /// Other cases - not expected 
        /// </summary>
        /// <param name="failedTask"></param>
        public void OnNext(IFailedTask failedTask)
        {
            Logger.Log(Level.Warning, "{0}: {1} and message: {2} from endpoint {3} with systemState {4} in retry#: {5}.", FailedTaskMessage, failedTask.Id, failedTask.Message, GetEndPointFromContext(failedTask.GetActiveContext()), _systemState.CurrentState, _numberOfRetries);
            lock (_lock)
            {
                using (Logger.LogFunction("IMRUDriver::IFailedTask"))
                {
                    if (_evaluatorsForceClosed.Contains(failedTask.GetActiveContext().Value.EvaluatorId))
                    {
                        Logger.Log(Level.Info, "Evaluator {0} has been closed after task {1} timeout, ignoring IFailedTask event..", failedTask.GetActiveContext().Value.EvaluatorId, failedTask.Id);
                        return;
                    }
                    switch (_systemState.CurrentState)
                    {
                        case SystemState.SubmittingTasks:
                        case SystemState.TasksRunning:
                            // When the event FailedNode happens, change the system state to ShuttingDown
                            _systemState.MoveNext(SystemStateEvent.FailedNode);
                            _taskManager.RecordFailedTaskDuringRunningOrSubmissionState(failedTask);
                            _taskManager.CloseAllRunningTasks(TaskManager.CloseTaskByDriver);
                            TryRecovery();
                            break;

                        case SystemState.TasksCompleted:
                            _taskManager.RecordFailedTaskDuringRunningOrSubmissionState(failedTask);
                            Logger.Log(Level.Info, "The Job has been completed. So ignoring the Task {0} failure.", failedTask.Id);
                            break;

                        case SystemState.ShuttingDown:
                            _taskManager.RecordFailedTaskDuringSystemShuttingDownState(failedTask);
                            TryRecovery();
                            break;

                        default:
                            UnexpectedState(failedTask.Id, "IFailedTask");
                            break;
                    }
                }
            }
        }
        #endregion IFailedTask

        private void TimeoutMonitor(object source, ElapsedEventArgs e)
        {
            Logger.Log(Level.Info, "Entering TimeoutMonitor at {0}", DateTime.Now);
            lock (_lock)
            {
                switch (_systemState.CurrentState)
                {
                    // TODO: Handle time out if ActiveContexts are not received in timeout limit
                    case SystemState.WaitingForEvaluator:
                        break;

                    // TODO: Handle time out if RunningTasks are not received in timeout limit
                    case SystemState.SubmittingTasks:
                        break;

                    // TODO: Handle time out if CompletedTasks are not received in timeout limit
                    case SystemState.TasksRunning:
                        break;

                    // Handle timeout for closing tasks
                    case SystemState.ShuttingDown:
                        Logger.Log(Level.Info, "_taskManager.AverageClosingTime {0}, _minTaskWaitingForCloseTimeout: {1}", _taskManager.AverageClosingTime(), _minTaskWaitingForCloseTimeout);
                        int taskClosingTimeout = Math.Max(_minTaskWaitingForCloseTimeout, _taskManager.AverageClosingTime() * TaskWaitingForCloseTimeFactor);
                        var waitingTasks = _taskManager.TasksTimeoutInState(TaskState.TaskWaitingForClose, taskClosingTimeout);

                        if (waitingTasks.Any())
                        {
                            Logger.Log(Level.Info, "There are {0} tasks that timed out", waitingTasks.Count);
                            WaitingForCloseTaskNoResponseAction(waitingTasks);
                        }
                        break;

                    case SystemState.TasksCompleted:
                        break;

                    case SystemState.Fail:
                        break;
                }
            }
        }

        /// <summary>
        /// For tasks that are in WaitingForCloseState and has no response in specified timeout
        /// kill the evaluator and set the other states as if we received the FailedEvaluator
        /// Then try recovery
        /// </summary>
        /// <param name="tasks"></param>
        private void WaitingForCloseTaskNoResponseAction(IList<KeyValuePair<string, TaskInfo>> tasks)
        {
            foreach (var t in tasks)
            {
                string evaluatorId = t.Value.ActiveContext.EvaluatorId;
                if (!_evaluatorsForceClosed.Contains(evaluatorId))
                {
                    _evaluatorsForceClosed.Add(evaluatorId);
                    Logger.Log(Level.Info,
                        "WaitingForCloseTask [{0}] has no response after timeout. Kill the evaluator: [{1}] and dispose the context: [{2}].",
                        t.Key,
                        evaluatorId,
                        t.Value.ActiveContext.Id);

                    t.Value.ActiveContext.Dispose();
                    var isMaster = _evaluatorManager.IsMasterEvaluatorId(evaluatorId);
                    _evaluatorManager.RecordFailedEvaluator(evaluatorId);
                    _contextManager.Remove(t.Value.ActiveContext.Id);
                    _taskManager.RecordKillClosingTask(t.Key);

                    // Push evaluator id back to PartitionIdProvider if it is not master
                    if (!isMaster)
                    {
                        _serviceAndContextConfigurationProvider.RemoveEvaluatorIdFromPartitionIdProvider(evaluatorId);
                    }
                }
            }
            TryRecovery();
        }

        public void OnNext(IJobCancelled value)
        {
            lock (_lock)
            {
                _cancelEvent = value;
                _systemState.MoveNext(SystemStateEvent.NotRecoverable);
                FailAction();
            }
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        private void UnexpectedState(string id, string eventName)
        {
            var msg = string.Format(CultureInfo.InvariantCulture,
                "Received {0} for [{1}], but system status is {2}.",
                eventName,
                id,
                _systemState.CurrentState);
            Exceptions.Throw(new IMRUSystemException(msg), Logger);
        }

        /// <summary>
        /// If all the tasks are in final state, if the system is recoverable, start recovery
        /// else, change the system state to Fail then take Fail action
        /// </summary>
        private void TryRecovery()
        {
            if (_taskManager.AreAllTasksInFinalState())
            {
                if (IsRecoverable())
                {
                    _isFirstTry = false;
                    RecoveryAction();
                }
                else
                {
                    Logger.Log(Level.Warning, "The system is not recoverable, change the state to Fail.");
                    _systemState.MoveNext(SystemStateEvent.NotRecoverable);
                    FailAction();
                }
            }
        }

        private string GetMapperTaskIdByEvaluatorId(string evaluatorId)
        {
            return string.Format("{0}-{1}-{2}",
                IMRUConstants.MapTaskPrefix,
                _serviceAndContextConfigurationProvider.GetPartitionIdByEvaluatorId(evaluatorId),
                _numberOfRetries);
        }

        /// <summary>
        /// This method is called when all the tasks are successfully completed. 
        /// </summary>
        private void DoneAction()
        {
            Logger.Log(Level.Info, "Shutting down Evaluators!!!");
            ShutDownAllEvaluators();
            Logger.Log(Level.Info, "{0} done in retry {1}!!!", DoneActionPrefix, _numberOfRetries);
            DisposeResources();
        }

        /// <summary>
        /// This method is called when there are failures and the system is not recoverable. 
        /// </summary>
        private void FailAction()
        {
            ShutDownAllEvaluators();
            
            var failMessage = _cancelEvent != null
                    ? string.Format(CultureInfo.InvariantCulture,
                        "{0} Job cancelled at {1}. cancellation message: {2}",
                        FailActionPrefix, _cancelEvent.Timestamp.ToString("u"), _cancelEvent.Message)
                    : string.Format(CultureInfo.InvariantCulture,
                        "{0} The system cannot be recovered after {1} retries. NumberofFailedMappers in the last try is {2}, master evaluator failed is {3}.",
                        FailActionPrefix, _numberOfRetries, _evaluatorManager.NumberofFailedMappers(), _evaluatorManager.IsMasterEvaluatorFailed());

            DisposeResources();
            Exceptions.Throw(new ApplicationException(failMessage), Logger);
        }

        /// <summary>
        /// Dispose resources
        /// </summary>
        private void DisposeResources()
        {
            lock (_disposableResources)
            {
                _disposableResources.ForEach(handle =>
                {
                    try
                    {
                        handle.Dispose();
                    }
                    catch (Exception ex)
                    {
                        Logger.Log(Level.Error, "Failed to dispose a resource: {0}", ex);
                    }
                });

                _disposableResources.Clear();
            }
        }

        /// <summary>
        /// Shuts down evaluators
        /// </summary>
        private void ShutDownAllEvaluators()
        {
            foreach (var context in _contextManager.ActiveContexts)
            {
                Logger.Log(Level.Verbose, "Disposing active context: {0}", context.Id);
                context.Dispose();
            }
        }

        /// <summary>
        /// This method is called for recovery. It resets Failed Evaluators and changes state to WaitingForEvaluator
        /// If there is no failed mappers, meaning the recovery is caused by failed tasks, resubmit all the tasks. 
        /// Else, based on the number of failed evaluators, requests missing map evaluators
        /// </summary>
        private void RecoveryAction()
        {
            lock (_lock)
            {
                _numberOfRetries++;
                var msg = string.Format(CultureInfo.InvariantCulture,
                    "Start recovery with _numberOfRetryForFaultTolerant {0}, NumberofFailedMappersToRequest {1}.",
                    _numberOfRetries,
                    _evaluatorManager.MappersToRequest());
                Logger.Log(Level.Info, msg);

                _systemState.MoveNext(SystemStateEvent.Recover);

                var mappersToRequest = _evaluatorManager.MappersToRequest();
                _evaluatorManager.ResetFailedEvaluators();

                if (mappersToRequest == 0)
                {
                    Logger.Log(Level.Info, "There is no failed Evaluator in this recovery but failed tasks.");
                    if (_contextManager.AreAllContextsReceived)
                    {
                        OnNext(_contextManager.ActiveContexts);
                    }
                    else
                    {
                        Exceptions.Throw(new IMRUSystemException("In recovery, there are no Failed evaluators but not all the contexts are received"), Logger);
                    }
                }
                else
                {
                    Logger.Log(Level.Info, "Requesting {0} map Evaluators.", mappersToRequest);
                    _evaluatorManager.RequestMapEvaluators(mappersToRequest);
                }
            }
        }

        /// <summary>
        /// Checks if the system is recoverable.
        /// </summary>
        /// <returns></returns>
        private bool IsRecoverable()
        {
            var msg = string.Format(CultureInfo.InvariantCulture,
                "IsRecoverable: _numberOfRetryForFaultTolerant {0}, NumberofFailedMappers {1}, NumberOfAppErrors {2}, IsMasterEvaluatorFailed {3} AllowedNumberOfEvaluatorFailures {4}, _maxRetryNumberForFaultTolerant {5}.",
                _numberOfRetries,
                _evaluatorManager.NumberofFailedMappers(),
                _taskManager.NumberOfAppErrors(),
                _evaluatorManager.IsMasterEvaluatorFailed(),
                _evaluatorManager.AllowedNumberOfEvaluatorFailures,
                _maxRetryNumberForFaultTolerant);
            Logger.Log(Level.Info, msg);

            return !_evaluatorManager.ExceededMaximumNumberOfEvaluatorFailures()
                && _taskManager.NumberOfAppErrors() == 0
                && !_evaluatorManager.IsMasterEvaluatorFailed()
                && _numberOfRetries < _maxRetryNumberForFaultTolerant;
        }

        /// <summary>
        /// Generates map task configuration given the active context.
        /// Merge configurations of all the inputs to the MapTaskHost.
        /// </summary>
        /// <param name="activeContext">Active context to which task needs to be submitted</param>
        /// <param name="taskId">Task Id</param>
        /// <returns>Map task configuration</returns>
        private IConfiguration GetMapperTaskConfiguration(IActiveContext activeContext, string taskId)
        {
            IConfiguration mapSpecificConfig;

            if (!_perMapperConfigurationStack.TryPop(out mapSpecificConfig))
            {
                Exceptions.Throw(
                    new IMRUSystemException(string.Format("No per map configuration exist for the active context {0}",
                        activeContext.Id)),
                    Logger);
            }

            return TangFactory.GetTang()
                .NewConfigurationBuilder(TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, taskId)
                    .Set(TaskConfiguration.Task, GenericType<MapTaskHost<TMapInput, TMapOutput>>.Class)
                    .Set(TaskConfiguration.OnClose, GenericType<MapTaskHost<TMapInput, TMapOutput>>.Class)
                    .Build(),
                    _configurationManager.MapFunctionConfiguration,
                    mapSpecificConfig,
                    GetGroupCommConfiguration())
                .BindNamedParameter<InvokeGC, bool>(GenericType<InvokeGC>.Class, _invokeGC.ToString())
                .Build();
        }

        /// <summary>
        /// Generates the update task configuration.
        /// Merge configurations of all the inputs to the UpdateTaskHost.
        /// </summary>
        /// <returns>Update task configuration</returns>
        private IConfiguration GetMasterTaskConfiguration(string taskId)
        {
            var partialTaskConf =
                TangFactory.GetTang()
                    .NewConfigurationBuilder(TaskConfiguration.ConfigurationModule
                        .Set(TaskConfiguration.Identifier,
                            taskId)
                        .Set(TaskConfiguration.Task,
                            GenericType<UpdateTaskHost<TMapInput, TMapOutput, TResult>>.Class)
                        .Set(TaskConfiguration.OnClose,
                            GenericType<UpdateTaskHost<TMapInput, TMapOutput, TResult>>.Class)
                        .Build(),
                        _configurationManager.UpdateFunctionConfiguration,
                        _configurationManager.ResultHandlerConfiguration,
                        GetGroupCommConfiguration())
                    .BindNamedParameter<InvokeGC, bool>(GenericType<InvokeGC>.Class, _invokeGC.ToString())
                    .Build();

            // This piece of code basically checks if user has given any implementation 
            // of IIMRUResultHandler. If not then bind it to default implementation which 
            // does nothing. For interfaces with generic type we cannot assign default 
            // implementation.
            try
            {
                TangFactory.GetTang()
                    .NewInjector(partialTaskConf)
                    .GetInstance<IIMRUResultHandler<TResult>>();
            }
            catch (InjectionException)
            {
                partialTaskConf = TangFactory.GetTang().NewConfigurationBuilder(partialTaskConf)
                    .BindImplementation(GenericType<IIMRUResultHandler<TResult>>.Class,
                        GenericType<DefaultResultHandler<TResult>>.Class)
                    .Build();
                Logger.Log(Level.Info,
                    "User has not given any way to handle IMRU result, defaulting to ignoring it");
            }
            return partialTaskConf;
        }

        /// <summary>
        /// Creates the group communication configuration to be added to the tasks
        /// </summary>
        /// <returns>The group communication configuration</returns>
        private IConfiguration GetGroupCommConfiguration()
        {
            var codecConfig =
                TangFactory.GetTang()
                    .NewConfigurationBuilder(
                        StreamingCodecConfiguration<MapInputWithControlMessage<TMapInput>>.Conf.Set(
                            StreamingCodecConfiguration<MapInputWithControlMessage<TMapInput>>.Codec,
                            GenericType<MapInputWithControlMessageCodec<TMapInput>>.Class).Build(),
                        StreamingCodecConfigurationMinusMessage<TMapOutput>.Conf.Build(),
                        _configurationManager.UpdateFunctionCodecsConfiguration)
                    .Build();

            return Configurations.Merge(_groupCommDriver.GetServiceConfiguration(), codecConfig);
        }

        /// <summary>
        /// Adds broadcast and reduce operators to the default communication group
        /// </summary>
        private ICommunicationGroupDriver AddCommunicationGroupWithOperators()
        {
            var reduceFunctionConfig = _configurationManager.ReduceFunctionConfiguration;
            var mapOutputPipelineDataConverterConfig = _configurationManager.MapOutputPipelineDataConverterConfiguration;
            var mapInputPipelineDataConverterConfig = _configurationManager.MapInputPipelineDataConverterConfiguration;

            // TODO check the specific exception type 
            try
            {
                TangFactory.GetTang()
                    .NewInjector(mapInputPipelineDataConverterConfig)
                    .GetInstance<IPipelineDataConverter<TMapInput>>();

                mapInputPipelineDataConverterConfig =
                    TangFactory.GetTang()
                        .NewConfigurationBuilder(mapInputPipelineDataConverterConfig)
                        .BindImplementation(
                            GenericType<IPipelineDataConverter<MapInputWithControlMessage<TMapInput>>>.Class,
                            GenericType<MapInputwithControlMessagePipelineDataConverter<TMapInput>>.Class)
                        .Build();
            }
            catch (Exception)
            {
                mapInputPipelineDataConverterConfig = TangFactory.GetTang()
                    .NewConfigurationBuilder()
                    .BindImplementation(
                        GenericType<IPipelineDataConverter<MapInputWithControlMessage<TMapInput>>>.Class,
                        GenericType<DefaultPipelineDataConverter<MapInputWithControlMessage<TMapInput>>>.Class)
                    .Build();
            }

            try
            {
                TangFactory.GetTang()
                    .NewInjector(mapOutputPipelineDataConverterConfig)
                    .GetInstance<IPipelineDataConverter<TMapOutput>>();
            }
            catch (Exception)
            {
                mapOutputPipelineDataConverterConfig =
                    TangFactory.GetTang()
                        .NewConfigurationBuilder()
                        .BindImplementation(GenericType<IPipelineDataConverter<TMapOutput>>.Class,
                            GenericType<DefaultPipelineDataConverter<TMapOutput>>.Class)
                        .Build();
            }

            var commGroup =
                _groupCommDriver.NewCommunicationGroup(IMRUConstants.CommunicationGroupName, _totalMappers + 1)
                    .AddBroadcast<MapInputWithControlMessage<TMapInput>>(
                        IMRUConstants.BroadcastOperatorName,
                        _groupCommDriver.MasterTaskId,
                        TopologyTypes.Tree,
                        mapInputPipelineDataConverterConfig)
                    .AddReduce<TMapOutput>(
                        IMRUConstants.ReduceOperatorName,
                        _groupCommDriver.MasterTaskId,
                        TopologyTypes.Tree,
                        reduceFunctionConfig,
                        mapOutputPipelineDataConverterConfig)
                    .Build();

            return commGroup;
        }

        /// <summary>
        /// Construct the stack of map configuration which is specific to each mapper. If user does not 
        /// specify any then its empty configuration
        /// </summary>
        /// <param name="totalMappers">Total mappers</param>
        /// <returns>Stack of configuration</returns>
        private ConcurrentStack<IConfiguration> ConstructPerMapperConfigStack(int totalMappers)
        {
            var perMapperConfiguration = new ConcurrentStack<IConfiguration>();
            for (int i = 0; i < totalMappers; i++)
            {
                var emptyConfig = TangFactory.GetTang().NewConfigurationBuilder().Build();
                IConfiguration config = _perMapperConfigs.Aggregate(emptyConfig,
                    (current, configGenerator) =>
                        Configurations.Merge(current, configGenerator.GetMapperConfiguration(i, totalMappers)));
                perMapperConfiguration.Push(config);
            }
            return perMapperConfiguration;
        }

        /// <summary>
        /// look up endpoint for given id
        /// </summary>
        /// <param name="taskId">Registered identifier in name server></param>
        /// <returns></returns>
        private string GetEndPointFromTaskId(string taskId)
        {
            List<string> t = new List<string>();
            t.Add(taskId);
            var ips = _nameServer.Lookup(t);
            if (ips.Count > 0)
            {
                var ip = ips.FirstOrDefault();
                if (ip != null)
                {
                    return ip.Endpoint.ToString();
                }
            }
            return null;
        }

        private string GetEndPoint(IFailedTask failedTask)
        { 
            return GetEndPointFromTaskId(failedTask.Id) ?? GetEndPointFromContext(failedTask.GetActiveContext()); 
        }

        private string GetEndPointFromContext(IFailedContext context)
        { 
            if (context == null || context.EvaluatorDescriptor == null || context.EvaluatorDescriptor.NodeDescriptor == null) 
            { 
                return null; 
            } 
            return context.EvaluatorDescriptor.NodeDescriptor.HostName; 
        } 
 
        private string GetEndPointFromContext(Optional<IActiveContext> context)
        { 
            if (!context.IsPresent() || context.Value == null || context.Value.EvaluatorDescriptor == null || context.Value.EvaluatorDescriptor.NodeDescriptor == null) 
            { 
                return null; 
            } 
            return context.Value.EvaluatorDescriptor.NodeDescriptor.HostName; 
        }

        /// <summary>
        /// Ensure the Timer is disposed when the driver object is deleted
        /// </summary>
        ~IMRUDriver()
        {
            if (_timeoutMonitorTimer != null)
            {
                _timeoutMonitorTimer.Stop();
                _timeoutMonitorTimer.Dispose();
                _timeoutMonitorTimer = null;
            }
        }
    }
}