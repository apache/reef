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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
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
        IObserver<IEnumerable<IActiveContext>>
    {
        private static readonly Logger Logger =
            Logger.GetLogger(typeof(IMRUDriver<TMapInput, TMapOutput, TResult, TPartitionType>));

        private readonly ConfigurationManager _configurationManager;
        private readonly int _totalMappers;
        private readonly IGroupCommDriver _groupCommDriver;
        private ConcurrentStack<IConfiguration> _perMapperConfigurationStack;
        private readonly ISet<IPerMapperConfigGenerator> _perMapperConfigs;
        private readonly bool _invokeGC;
        private readonly ServiceAndContextConfigurationProvider<TMapInput, TMapOutput, TPartitionType> _serviceAndContextConfigurationProvider;

        /// <summary>
        /// The lock for the driver. 
        /// </summary>
        private readonly object _lock = new object();

        /// <summary>
        /// Manages Tasks, maintains task states and responsible for task submission for the driver.
        /// </summary>
        private readonly TaskManager _taskManager;

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
        private int _numberOfRetriesForFaultTolerant = 0;

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
            [Parameter(typeof(InvokeGC))] bool invokeGC,
            IGroupCommDriver groupCommDriver)
        {
            _configurationManager = configurationManager;
            _groupCommDriver = groupCommDriver;
            _perMapperConfigs = perMapperConfigs;
            _totalMappers = dataSet.Count;
            _invokeGC = invokeGC;
            _maxRetryNumberForFaultTolerant = maxRetryNumberInRecovery;

            _contextManager = new ActiveContextManager(_totalMappers + 1);
            _contextManager.Subscribe(this);

            var updateSpec = new EvaluatorSpecification(memoryForUpdateTask, coresForUpdateTask);
            var mapperSpec = new EvaluatorSpecification(memoryPerMapper, coresPerMapper);
            var allowedFailedEvaluators = (int)(failedEvaluatorsFraction * _totalMappers);
            _evaluatorManager = new EvaluatorManager(_totalMappers + 1, allowedFailedEvaluators, evaluatorRequestor, updateSpec, mapperSpec);

            _taskManager = new TaskManager(_totalMappers + 1, _groupCommDriver.MasterTaskId);
            _systemState = new SystemStateMachine();
            _serviceAndContextConfigurationProvider =
                new ServiceAndContextConfigurationProvider<TMapInput, TMapOutput, TPartitionType>(dataSet);

            var msg =
                string.Format(CultureInfo.InvariantCulture, "map task memory:{0}, update task memory:{1}, map task cores:{2}, update task cores:{3}, maxRetry {4}.",
                    memoryPerMapper,
                    memoryForUpdateTask,
                    coresPerMapper,
                    coresForUpdateTask,
                    maxRetryNumberInRecovery);
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
            _evaluatorManager.RequestMapEvaluators(_totalMappers);
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
            Logger.Log(Level.Verbose, "AllocatedEvaluator EvaluatorBatchId [{0}], memory [{1}], systemState {2}.", allocatedEvaluator.EvaluatorBatchId, allocatedEvaluator.GetEvaluatorDescriptor().Memory, _systemState.CurrentState);
            lock (_lock)
            {
                switch (_systemState.CurrentState)
                {
                    case SystemState.WaitingForEvaluator:
                        _evaluatorManager.AddAllocatedEvaluator(allocatedEvaluator);
                        SubmitContextAndService(allocatedEvaluator);
                        break;
                    case SystemState.Fail:                        
                        Logger.Log(Level.Info, "Receiving IAllocatedEvaluator event, but system is in FAIL state, ignore it.");
                        allocatedEvaluator.Dispose();
                        break;
                    default:
                        UnexpectedState(allocatedEvaluator.Id, "IAllocatedEvaluator");
                        break;
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
            if (_evaluatorManager.IsEvaluatorForMaster(allocatedEvaluator))
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
            Logger.Log(Level.Verbose, "Received Active Context {0}, systemState {1}.", activeContext.Id, _systemState.CurrentState);
            lock (_lock)
            {
                switch (_systemState.CurrentState)
                {
                    case SystemState.WaitingForEvaluator:
                        _contextManager.Add(activeContext);
                        break;
                    case SystemState.Fail:
                        Logger.Log(Level.Info, "Received IActiveContext event, but system is in FAIL state. Closing the context.");
                        activeContext.Dispose();
                        break;
                    default:
                        UnexpectedState(activeContext.Id, "IActiveContext");
                        break;
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
            Logger.Log(Level.Info, "Received event from ActiveContextManager with NumberOfActiveContexts:" + value);
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
            Logger.Log(Level.Info, "SubmitTasks with system state :" + _systemState.CurrentState);
            if (!_isFirstTry)
            {
                _taskManager.Reset();
                _groupCommDriver.RemoveCommunicationGroup(IMRUConstants.CommunicationGroupName);
            }

            var commGroup = AddCommunicationGroupWithOperators();
            _perMapperConfigurationStack = ConstructPerMapperConfigStack(_totalMappers);

            var taskIdAndContextMapping = new Dictionary<string, IActiveContext>();
            foreach (var activeContext in activeContexts)
            {
                var taskId = _evaluatorManager.IsMasterEvaluatorId(activeContext.EvaluatorId) ?
                    _groupCommDriver.MasterTaskId :
                    GetMapperTaskIdByEvaluatorId(activeContext.EvaluatorId);
                commGroup.AddTask(taskId);
                taskIdAndContextMapping.Add(taskId, activeContext);
            }

            foreach (var mapping in taskIdAndContextMapping)
            {
                var taskConfig = _evaluatorManager.IsMasterEvaluatorId(mapping.Value.EvaluatorId) ?
                    GetMasterTaskConfiguration(mapping.Key) :
                    GetMapperTaskConfiguration(mapping.Value, mapping.Key);
                var groupCommTaskConfiguration = _groupCommDriver.GetGroupCommTaskConfiguration(mapping.Key);
                var mergedTaskConf = Configurations.Merge(taskConfig, groupCommTaskConfiguration);
                _taskManager.AddTask(mapping.Key, mergedTaskConf, mapping.Value);
            }
            _taskManager.SubmitTasks();
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
            Logger.Log(Level.Verbose, "Received IRunningTask {0} at SystemState {1} in retry # {2}.", runningTask.Id, _systemState.CurrentState, _numberOfRetriesForFaultTolerant);
            lock (_lock)
            {
                switch (_systemState.CurrentState)
                {
                    case SystemState.SubmittingTasks:
                        _taskManager.RecordRunningTask(runningTask);
                        if (_taskManager.AreAllTasksRunning())
                        {
                            _systemState.MoveNext(SystemStateEvent.AllTasksAreRunning);
                            Logger.Log(Level.Info, "All tasks are running, SystemState {0}", _systemState.CurrentState);
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
        #endregion IRunningTask

        #region ICompletedTask
        /// <summary>
        /// ICompletedTask handler. It is called when a task is completed. The following action will be taken based on the System State:
        /// Case TasksRunning
        ///     Updates task state to TaskCompleted
        ///     If all tasks are completed, sets system state to TasksCompleted and then go to Done action
        /// Case ShuttingDown
        ///     Updates task state to TaskCompleted
        ///     Try to recover
        /// Other cases - not expected 
        /// </summary>
        /// <param name="completedTask">The link to the completed task</param>
        public void OnNext(ICompletedTask completedTask)
        {
            Logger.Log(Level.Verbose, "Received ICompletedTask {0}, with systemState {1} in retry# {2}.", completedTask.Id, _systemState.CurrentState, _numberOfRetriesForFaultTolerant);
            lock (_lock)
            {
                switch (_systemState.CurrentState)
                {
                    case SystemState.TasksRunning:
                        _taskManager.RecordCompletedTask(completedTask);
                        if (_taskManager.AreAllTasksCompleted())
                        {
                            _systemState.MoveNext(SystemStateEvent.AllTasksAreCompleted);
                            Logger.Log(Level.Info, "All tasks are completed, systemState {0}", _systemState.CurrentState);
                            DoneAction();
                        }
                        break;
                    case SystemState.ShuttingDown:
                        // The task might be in running state or waiting for close, set it to complete anyway to make its state final
                        _taskManager.RecordCompletedTask(completedTask);
                        TryRecovery();
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
        /// If we get all completed tasks then ignore the failure. Otherwise, take the following actions based on the system state: 
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
            Logger.Log(Level.Warning, "Received IFailedEvaluator {0}, with systemState {1} in retry# {2} with Exception: {3}.", failedEvaluator.Id, _systemState.CurrentState, _numberOfRetriesForFaultTolerant, failedEvaluator.EvaluatorException);
            lock (_lock)
            {
                if (_taskManager.AreAllTasksCompleted())
                {
                    Logger.Log(Level.Verbose, "All IMRU tasks have been completed. So ignoring the Evaluator {0} failure.", failedEvaluator.Id);
                    return;
                }

                var isMaster = _evaluatorManager.IsMasterEvaluatorId(failedEvaluator.Id);
                _evaluatorManager.RecordFailedEvaluator(failedEvaluator.Id);
                _contextManager.RemoveFailedContextInFailedEvaluator(failedEvaluator);

                switch (_systemState.CurrentState)
                {
                    case SystemState.WaitingForEvaluator:
                        if (!_evaluatorManager.ReachedMaximumNumberOfEvaluatorFailures())
                        {
                            if (isMaster)
                            {
                                Logger.Log(Level.Info, "Requesting a master Evaluator.");
                                _evaluatorManager.RequestUpdateEvaluator();
                            }
                            else
                            {
                                _serviceAndContextConfigurationProvider.RemoveEvaluatorIdFromPartitionIdProvider(failedEvaluator.Id);
                                Logger.Log(Level.Info, "Requesting mapper Evaluators.");
                                _evaluatorManager.RequestMapEvaluators(1);
                            }
                        }
                        else
                        {
                            Logger.Log(Level.Error, "The system is not recoverable, change the state to Fail.");
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
                            _serviceAndContextConfigurationProvider.RemoveEvaluatorIdFromPartitionIdProvider(failedEvaluator.Id);
                        }

                        TryRecovery();
                        break;

                    case SystemState.ShuttingDown:
                        _taskManager.RecordTaskFailWhenReceivingFailedEvaluator(failedEvaluator);

                        // Push evaluator id back to PartitionIdProvider if it is not master
                        if (!isMaster)
                        {
                            _serviceAndContextConfigurationProvider.RemoveEvaluatorIdFromPartitionIdProvider(failedEvaluator.Id);
                        }
                        TryRecovery();
                        break;

                    case SystemState.Fail:
                        break;

                    default:
                        UnexpectedState(failedEvaluator.Id, "IFailedEvaluator");
                        break;
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
            lock (_lock)
            {
                if (_taskManager.AreAllTasksCompleted())
                {
                    Logger.Log(Level.Info, "Context with Id: {0} failed but IMRU tasks are completed. So ignoring.", failedContext.Id);
                    return;
                }

                var msg = string.Format("Context with Id: {0} failed with Evaluator id: {1}", failedContext.Id, failedContext.EvaluatorId);
                Exceptions.Throw(new Exception(msg), Logger);
            }
        }
        #endregion IFailedContext

        #region IFailedTask
        /// <summary>
        /// IFailedTask handler. It specifies what to do when task fails.
        /// If we get all completed tasks then ignore the failure. Otherwise take the following actions based on the System state:
        /// Case SubmittingTasks/TasksRunning
        ///     This is the first failure received
        ///     Changes the system state to ShuttingDown
        ///     Record failed task in TaskManager
        ///     Closes all the other running tasks and set their state to TaskWaitingForClose
        ///     Try to recover
        /// Case ShuttingDown
        ///     This happens when we have received either FailedEvaluator or FailedTask, some tasks are running some are in closing.
        ///     Record failed task in TaskManager.
        ///     Try to recover
        /// Other cases - not expected 
        /// </summary>
        /// <param name="failedTask"></param>
        public void OnNext(IFailedTask failedTask)
        {
            Logger.Log(Level.Warning, "Receive IFailedTask with Id: {0} and message: {1} in retry#: {2}.", failedTask.Id, failedTask.Message, _numberOfRetriesForFaultTolerant);
            lock (_lock)
            {
                if (_taskManager.AreAllTasksCompleted())
                {
                    Logger.Log(Level.Info, "Task with Id: {0} failed but all IMRU tasks are completed. So ignoring.", failedTask.Id);
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
        #endregion IFailedTask

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
            return string.Format("{0}-{1}-Version0",
                IMRUConstants.MapTaskPrefix,
                _serviceAndContextConfigurationProvider.GetPartitionIdByEvaluatorId(evaluatorId));
        }

        /// <summary>
        /// This method is called when all the tasks are successfully completed. 
        /// </summary>
        private void DoneAction()
        {
            ShutDownAllEvaluators();
        }

        /// <summary>
        /// This method is called when there are failures and the system is not recoverable. 
        /// </summary>
        private void FailAction()
        {
            ShutDownAllEvaluators();
            var msg = string.Format(CultureInfo.InvariantCulture,
                "The system cannot be recovered after {0} retries. NumberofFailedMappers in the last try is {1}.",
                _numberOfRetriesForFaultTolerant, _evaluatorManager.NumberofFailedMappers());
            Exceptions.Throw(new ApplicationException(msg), Logger);
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
                _numberOfRetriesForFaultTolerant++;
                Logger.Log(Level.Info, "Start recovery with _numberOfRetryForFaultTolerant:" + _numberOfRetriesForFaultTolerant);
                _systemState.MoveNext(SystemStateEvent.Recover);

                var mappersToRequest = _evaluatorManager.NumberofFailedMappers();
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
            return !_evaluatorManager.ReachedMaximumNumberOfEvaluatorFailures()
                && _taskManager.NumberOfAppErrors() == 0
                && !_evaluatorManager.IsMasterEvaluatorFailed()
                && _numberOfRetriesForFaultTolerant < _maxRetryNumberForFaultTolerant;
        }

        /// <summary>
        /// Generates map task configuration given the active context. S
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
    }
}