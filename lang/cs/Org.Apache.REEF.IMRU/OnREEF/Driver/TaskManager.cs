﻿// Licensed to the Apache Software Foundation (ASF) under one
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
using System.Globalization;
using System.Linq;
using System.Text;
using Org.Apache.REEF.Common.Runtime.Evaluator.Task;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.IMRU.OnREEF.Driver.StateMachine;
using Org.Apache.REEF.IMRU.OnREEF.IMRUTasks;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// Manages Tasks, maintains task states and responsible for task submission
    /// </summary>
    [NotThreadSafe]
    internal sealed class TaskManager
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TaskManager));

        /// <summary>
        /// Error messages thrown in IMRU tasks when an exception happens
        /// </summary>
        internal const string TaskAppError = "TaskAppError";
        internal const string TaskSystemError = "TaskSystemError";
        internal const string TaskGroupCommunicationError = "TaskGroupCommunicationError";
        internal const string TaskEvaluatorError = "TaskEvaluatorError";

        /// <summary>
        /// Message sending from driver to evaluator to close a running task
        /// </summary>
        internal const string CloseTaskByDriver = "CloseTaskByDriver";

        /// <summary>
        /// Error message in Task exception to show the task received close event
        /// </summary>
        internal const string TaskKilledByDriver = "TaskKilledByDriver";

        /// <summary>
        /// This Dictionary contains task information. The key is the Id of the Task, the value is TaskInfo which contains
        /// task state, task configuration, and active context that the task is running on. 
        /// </summary>
        private readonly IDictionary<string, TaskInfo> _tasks = new Dictionary<string, TaskInfo>();

        /// <summary>
        /// This Dictionary keeps all the running tasks. The key is the Task Id and the value is IRunningTask. 
        /// After a task is running, it will be added to this collection. After the task is requested to close, 
        /// or fails, completed, it will be removed from this collection. 
        /// </summary>
        private readonly IDictionary<string, IRunningTask> _runningTasks = new Dictionary<string, IRunningTask>();

        /// <summary>
        /// Total expected tasks
        /// </summary>
        private readonly int _totalExpectedTasks;

        /// <summary>
        /// Master tasks Id is set in the IGroupCommDriver. It must be the same Id used in the TaskManager.
        /// </summary>
        private readonly string _masterTaskId;

        /// <summary>
        /// Total number of Application error received from tasks
        /// </summary>
        private int _numberOfAppErrors = 0;

        /// <summary>
        /// Indicate if master task is completed running properly
        /// </summary>
        private bool _masterTaskCompletedRunning = false;

        /// <summary>
        /// Creates a TaskManager with specified total number of tasks and master task id.
        /// Throws IMRUSystemException if numTasks is smaller than or equals to 0 or masterTaskId is null.
        /// </summary>
        /// <param name="numTasks"></param>
        /// <param name="masterTaskId"></param>
        internal TaskManager(int numTasks, string masterTaskId)
        {
            if (numTasks <= 0)
            {
                Exceptions.Throw(new IMRUSystemException("Number of expected tasks must be positive"), Logger);
            }

            if (string.IsNullOrWhiteSpace(masterTaskId))
            {
                Exceptions.Throw(new IMRUSystemException("masterTaskId cannot be null"), Logger);
            }

            _totalExpectedTasks = numTasks;
            _masterTaskId = masterTaskId;
        }

        /// <summary>
        /// Adds a Task to the task collection
        /// Throws IMRUSystemException in the following cases:
        ///   taskId is already added 
        ///   taskConfiguration is null
        ///   activeContext is null
        ///   trying to add extra tasks
        ///   No Master Task is added in the collection
        /// </summary>
        /// <param name="taskId"></param>
        /// <param name="taskConfiguration"></param>
        /// <param name="activeContext"></param>
        internal void AddTask(string taskId, IConfiguration taskConfiguration, IActiveContext activeContext)
        {
            if (taskId == null)
            {
                Exceptions.Throw(new IMRUSystemException("The taskId is null."), Logger);
            }

            if (_tasks.ContainsKey(taskId))
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "The task [{0}] already exists.", taskId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            if (taskConfiguration == null)
            {
                Exceptions.Throw(new IMRUSystemException("The task configuration is null."), Logger);
            }

            if (activeContext == null)
            {
                Exceptions.Throw(new IMRUSystemException("The context is null."), Logger);
            }

            if (NumberOfTasks >= _totalExpectedTasks)
            {
                string msg = string.Format("Trying to add an additional Task {0}, but the total expected Task number {1} has been reached.", taskId, _totalExpectedTasks);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            _tasks.Add(taskId, new TaskInfo(new TaskStateMachine(), taskConfiguration, activeContext));

            if (NumberOfTasks == _totalExpectedTasks && !MasterTaskExists())
            {
                Exceptions.Throw(new IMRUSystemException("There is no master task added."), Logger);
            }
        }

        /// <summary>
        /// Returns the number of tasks in the task collection
        /// </summary>
        internal int NumberOfTasks
        {
            get { return _tasks.Count; }
        }

        /// <summary>
        /// This method is called when receiving IRunningTask event during task submitting.
        /// Adds the IRunningTask to the running tasks collection and update the task state to TaskRunning.
        /// Throws IMRUSystemException if running tasks already contains this task or tasks collection doesn't contain this task.
        /// </summary>
        /// <param name="runningTask"></param>
        internal void RecordRunningTask(IRunningTask runningTask)
        {
            if (_runningTasks.ContainsKey(runningTask.Id))
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "The task [{0}] already running.", runningTask.Id);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            if (!_tasks.ContainsKey(runningTask.Id))
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "The task [{0}] doesn't exist.", runningTask.Id);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            _runningTasks.Add(runningTask.Id, runningTask);
            UpdateState(runningTask.Id, TaskStateEvent.RunningTask);
        }

        /// <summary>
        /// This method is called at the beginning of the recovery.
        /// Clears the task collection, running task collection and resets the number of application error. 
        /// </summary>
        internal void Reset()
        {
            _tasks.Clear();
            _runningTasks.Clear();
            _numberOfAppErrors = 0;
        }

        /// <summary>
        /// This method is called when receiving ICompletedTask event during task running or system shutting down.
        /// If it is master task and if the master task was running, mark _masterTaskCompletedRunning true. That indicates 
        /// master task has successfully completed, which means the system has got the result from master task. 
        /// Removes the task from running tasks if it was running
        /// Changes the task state from RunningTask to CompletedTask if the task was running
        /// Change the task stat from TaskWaitingForClose to TaskClosedByDriver if the task was in TaskWaitingForClose state
        /// </summary>
        /// <param name="completedTask"></param>
        internal void RecordCompletedTask(ICompletedTask completedTask)
        {
            if (completedTask.Id.Equals(_masterTaskId))
            {
                if (GetTaskInfo(completedTask.Id).TaskState.CurrentState.Equals(TaskState.TaskRunning))
                {
                    _masterTaskCompletedRunning = true;
                }
            }
            _runningTasks.Remove(completedTask.Id);
            UpdateState(completedTask.Id, TaskStateEvent.CompletedTask);
        }

        /// <summary>
        /// This method is called when receiving IFailedTask event during task submitting or running
        /// Removes the task from running tasks if the task was running
        /// Updates the task state to fail based on the error message in the failed task
        /// </summary>
        /// <param name="failedTask"></param>
        internal void RecordFailedTaskDuringRunningOrSubmissionState(IFailedTask failedTask)
        {
            //// Remove the task from running tasks if it exists there
            _runningTasks.Remove(failedTask.Id);
            UpdateState(failedTask.Id, GetTaskErrorEventByExceptionType(failedTask));
        }

        /// <summary>
        /// This method is called when receiving IFailedTask event during system shutting down. 
        /// If the task failed because it receives the close command from driver, update the task state to TaskClosedByDriver.
        /// Task could fail by communication error or any other application or system error during this time, as long as it is not 
        /// TaskFailedByEvaluatorFailure, update the task state based on the error received. 
        /// </summary>
        /// <param name="failedTask"></param>
        internal void RecordFailedTaskDuringSystemShuttingDownState(IFailedTask failedTask)
        {
            Logger.Log(Level.Info, "RecordFailedTaskDuringSystemShuttingDownState, exceptionType: {0}", GetTaskErrorEventByExceptionType(failedTask).ToString());

            var taskState = GetTaskState(failedTask.Id);
            if (taskState == StateMachine.TaskState.TaskWaitingForClose)
            {
                UpdateState(failedTask.Id, TaskStateEvent.ClosedTask);
            }
            else if (taskState != StateMachine.TaskState.TaskFailedByEvaluatorFailure)
            {
                UpdateState(failedTask.Id, GetTaskErrorEventByExceptionType(failedTask));
            }
        }

        /// <summary>
        /// This method is called when receiving an IFailedEvaluator event during TaskSubmitted, TaskRunning or system shutting down.
        /// Removes the task from RunningTasks if the task associated with the FailedEvaluator is present and running. 
        /// Sets the task state to TaskFailedByEvaluatorFailure 
        /// </summary>
        /// <param name="failedEvaluator"></param>
        internal void RecordTaskFailWhenReceivingFailedEvaluator(IFailedEvaluator failedEvaluator)
        {
            if (failedEvaluator.FailedTask.IsPresent())
            {
                var taskId = failedEvaluator.FailedTask.Value.Id;
                var taskState = GetTaskState(taskId);
                if (taskState == StateMachine.TaskState.TaskRunning)
                {
                    if (!_runningTasks.ContainsKey(taskId))
                    {
                        var msg = string.Format(CultureInfo.InvariantCulture,
                            "The task [{0}] doesn't exist in Running Tasks.",
                            taskId);
                        Exceptions.Throw(new IMRUSystemException(msg), Logger);
                    }
                    _runningTasks.Remove(taskId);
                }

                UpdateState(taskId, TaskStateEvent.FailedTaskEvaluatorError);
            }
            else
            {
                var taskId = FindTaskAssociatedWithTheEvalutor(failedEvaluator.Id);
                var taskState = GetTaskState(taskId);
                if (taskState == StateMachine.TaskState.TaskSubmitted)
                {
                    UpdateState(taskId, TaskStateEvent.FailedTaskEvaluatorError);
                }
            }
        }

        private string FindTaskAssociatedWithTheEvalutor(string evaluatorId)
        {
            return _tasks.Where(e => e.Value.ActiveContext.EvaluatorId.Equals(evaluatorId)).Select(e => e.Key).FirstOrDefault();
        }

        /// <summary>
        /// Updates task state for a given taskId based on the task event
        /// </summary>
        /// <param name="taskId"></param>
        /// <param name="taskEvent"></param>
        private void UpdateState(string taskId, TaskStateEvent taskEvent)
        {
            GetTaskInfo(taskId).TaskState.MoveNext(taskEvent);
        }

        /// <summary>
        /// Returns true if master task has completed and produced result
        /// </summary>
        /// <returns></returns>
        internal bool IsMasterTaskCompletedRunning()
        {
            return _masterTaskCompletedRunning;
        }

        /// <summary>
        /// Checks if all the tasks are running.
        /// </summary>
        /// <returns></returns>
        internal bool AreAllTasksRunning()
        {
            return AreAllTasksInState(StateMachine.TaskState.TaskRunning) &&
                _runningTasks.Count == _totalExpectedTasks;
        }

        /// <summary>
        /// When master task is completed, that means the system has got the result expected 
        /// regardless of other mapper tasks returned or not. 
        /// </summary>
        /// <returns></returns>
        internal bool IsJobDone()
        {
            return IsMasterTaskCompletedRunning();
        }

        /// <summary>
        /// This method is called when receiving either IFailedEvaluator or IFailedTask event
        /// Driver tries to close all the running tasks and clean the running task collection in the end.
        /// If all the tasks are running, the total number of running tasks should be _totalExpectedTasks -1
        /// If this happens before all the tasks are running, then the total number of running tasks should smaller than _totalExpectedTasks -1
        /// If this happens when no task is running, the total number of running tasks could be 0
        /// </summary>
        internal void CloseAllRunningTasks(string closeMessage)
        {
            Logger.Log(Level.Verbose, "Closing [{0}] running tasks.", _runningTasks.Count);
            foreach (var runningTask in _runningTasks.Values)
            {
                runningTask.Dispose(Encoding.UTF8.GetBytes(closeMessage));
                UpdateState(runningTask.Id, TaskStateEvent.WaitingTaskToClose);
            }
            _runningTasks.Clear();
        }

        /// <summary>
        /// This method is called when receiving an IRunningTask event but system is either in shutting down or fail.
        /// In this case, the task should not be added in Running Tasks yet.
        /// Change the task state to TaskRunning if it is still in TaskSubmitted state
        /// Closes the IRunningTask 
        /// Then move the task state to WaitingTaskToClose
        /// Throw IMRUSystemException if runningTask is null or the running task is already added in the running task collection
        /// </summary>
        /// <param name="runningTask"></param>
        /// <param name="closeMessage"></param>
        internal void RecordRunningTaskDuringSystemFailure(IRunningTask runningTask, string closeMessage)
        {
            if (runningTask == null)
            {
                Exceptions.Throw(new IMRUSystemException("RunningTask is null."), Logger);
            }

            if (_runningTasks.ContainsKey(runningTask.Id))
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "The task [{0}] is already in running tasks.", runningTask.Id);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            UpdateState(runningTask.Id, TaskStateEvent.RunningTask);
            runningTask.Dispose(Encoding.UTF8.GetBytes(closeMessage));
            UpdateState(runningTask.Id, TaskStateEvent.WaitingTaskToClose);
        }

        /// <summary>
        /// Gets error type (encoded as TaskStateEvent) based on the exception type in IFailedTask.
        /// For unknown exceptions or exceptions that doesn't belong to defined IMRU task exceptions
        /// treat then as application error.
        /// </summary>
        /// <param name="failedTask"></param>
        /// <returns></returns>
        private TaskStateEvent GetTaskErrorEventByExceptionType(IFailedTask failedTask)
        {
            var exception = failedTask.AsError();
            var innerExceptionType = exception.InnerException != null ? exception.InnerException.GetType().ToString() : "InnerException null";
            var innerExceptionMsg = exception.InnerException != null ? exception.InnerException.Message : "No InnerException";


            if (failedTask.GetActiveContext().IsPresent())
            {
                Logger.Log(Level.Info, "GetTaskErrorEventByExceptionType: with task id: {0}, exception type {1}, innerException type {2}, InnerExceptionMessage {3}, evaluator id: {4}",
                     failedTask.Id,
                     exception.GetType(),
                     innerExceptionType,
                     innerExceptionMsg,
                     failedTask.GetActiveContext().Value.EvaluatorId);
            }
            else
            {
                Logger.Log(Level.Info, "GetTaskErrorEventByExceptionType: with task id: {0}, exception type {1}, innerException type {2}, InnerExceptionMessage {3}",
                     failedTask.Id,
                     exception.GetType(),
                     innerExceptionType,
                     innerExceptionMsg);
            }

            if (exception is IMRUTaskAppException)
            {
                _numberOfAppErrors++;
                return TaskStateEvent.FailedTaskAppError;
            }
            if (exception is IMRUTaskGroupCommunicationException)
            {
                return TaskStateEvent.FailedTaskCommunicationError;
            }
            if (exception is IMRUTaskSystemException)
            {
                return TaskStateEvent.FailedTaskSystemError;
            }

            // special case for communication error during group communication initialization
            if (exception is TaskClientCodeException)
            {
                // try extract cause and check whether it is InjectionException for GroupCommClient
                if (exception.InnerException != null &&
                    exception.InnerException is InjectionException &&
                    exception.InnerException.Message.Contains("GroupCommClient"))
                {
                    Logger.Log(Level.Info, "GetTaskErrorEventByExceptionType:FailedTaskCommunicationError with task id {0}", failedTask.Id);
                    return TaskStateEvent.FailedTaskCommunicationError;
                }
            }

            Logger.Log(Level.Info, "GetTaskErrorEventByExceptionType for un-hanlded exception with task id {0} and exception type {1}", failedTask.Id, exception.GetType());
            return TaskStateEvent.FailedTaskSystemError;
        }

        /// <summary>
        /// Returns the number of application error caused by FailedTask
        /// </summary>
        /// <returns></returns>
        internal int NumberOfAppErrors()
        {
            return _numberOfAppErrors;
        }

        /// <summary>
        /// Checks if all the tasks are in final states
        /// </summary>
        /// <returns></returns>
        internal bool AreAllTasksInFinalState()
        {
            var notInFinalState = _tasks.Where(t => !t.Value.TaskState.IsFinalState()).Take(5).ToList();
            var count = _tasks.Where(t => !t.Value.TaskState.IsFinalState()).Count();

            if (notInFinalState.Any())
            {
                Logger.Log(Level.Info, "Total tasks that are not in final state: {0}, and first 5 are:\r\n {1}", count, string.Join("\r\n", notInFinalState.Select(ToLog)));
            }
            else
            {
                Logger.Log(Level.Info, "All the tasks are in final state");
            }

            return !notInFinalState.Any();
        }

        private string ToLog(KeyValuePair<string, TaskInfo> t)
        {
            try
            {
                return string.Format("State={0}, taskId={1}, ContextId={2}, evaluatorId={3}, evaluatorHost={4}",
                    t.Value.TaskState.CurrentState,
                    t.Key,
                    t.Value.ActiveContext.Id,
                    t.Value.ActiveContext.EvaluatorId,
                    t.Value.ActiveContext.EvaluatorDescriptor.NodeDescriptor.HostName);
            }
            catch (Exception ex)
            {
                return string.Format("Failed to get task string: {0}", ex);
            }
        }

        /// <summary>
        /// Gets current state of the task
        /// </summary>
        /// <param name="taskId"></param>
        /// <returns></returns>
        internal TaskState GetTaskState(string taskId)
        {
            var taskInfo = GetTaskInfo(taskId);
            return taskInfo.TaskState.CurrentState;
        }

        /// <summary>
        /// Checks if all the tasks are in the state specified. 
        /// For example, passing TaskState.TaskRunning to check if all the tasks are in TaskRunning state
        /// </summary>
        /// <param name="taskState"></param>
        /// <returns></returns>
        internal bool AreAllTasksInState(TaskState taskState)
        {
            return _tasks.All(t => t.Value.TaskState.CurrentState == taskState);
        }

        /// <summary>
        /// Submit all the tasks
        /// Tasks will be submitted after all the tasks are added in the collection and master task exists
        /// IMRUSystemException will be thrown if not all the tasks are added or if there is no master task
        /// </summary>
        internal void SubmitTasks()
        {
            using (Logger.LogFunction("TaskManager::SubmitTasks"))
            {
                if (NumberOfTasks < _totalExpectedTasks || !MasterTaskExists())
                {
                    string msg =
                        string.Format(
                            "Trying to submit tasks but either master task doesn't exist or number of tasks [{0}] is smaller than expected number of tasks [{1}].",
                            NumberOfTasks,
                            _totalExpectedTasks);
                    Exceptions.Throw(new IMRUSystemException(msg), Logger);
                }

                SubmitTask(_masterTaskId);

                foreach (var taskId in _tasks.Keys)
                {
                    if (taskId.Equals(_masterTaskId))
                    {
                        continue;
                    }
                    SubmitTask(taskId);
                }
            }
        }

        private void SubmitTask(string taskId)
        {
            Logger.Log(Level.Info, "SubmitTask with task id: {0}.", taskId);
            var taskInfo = GetTaskInfo(taskId);
            taskInfo.ActiveContext.SubmitTask(taskInfo.TaskConfiguration);
            UpdateState(taskId, TaskStateEvent.SubmittedTask);
        }

        /// <summary>
        /// Checks if master task has been added
        /// </summary>
        /// <returns></returns>
        private bool MasterTaskExists()
        {
            return _tasks.ContainsKey(_masterTaskId);
        }

        /// <summary>
        /// Gets task Tuple based on the given taskId. 
        /// Throws IMRUSystemException if the task Tuple is not in the task collection.
        /// </summary>
        /// <param name="taskId"></param>
        /// <returns></returns>
        private TaskInfo GetTaskInfo(string taskId)
        {
            TaskInfo taskInfo;
            _tasks.TryGetValue(taskId, out taskInfo);
            if (taskInfo == null)
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "The task [{0}] does not exist in the task collection.", taskId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }
            return taskInfo;
        }
    }
}