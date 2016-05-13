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
using System.Globalization;
using System.Linq;
using System.Text;
using Org.Apache.REEF.Common;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.IMRU.OnREEF.Driver.StateMachine;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// Manages Tasks, maintains task states and responsible for task submission
    /// It covers the functionality in TaskStarter which will be not used in IMRU driver. 
    /// </summary>
    [NotThreadSafe]
    internal sealed class TaskManager
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TaskManager));

        internal const string TaskAppError = "TaskAppError";
        internal const string TaskSystemError = "TaskSystemError";
        internal const string TaskGroupCommunicationError = "TaskGroupCommunicationError";
        internal const string TaskEvaluatorError = "TaskEvaluatorError";
        internal const string TaskKilledByDriver = "TaskKilledByDriver";
        internal const string CloseTaskByDriver = "CloseTaskByDriver";

        /// <summary>
        /// This Dictionary contains task information. The key is the Id of the Task, the value is TaskInfo which contains
        /// task state, partial task configuration, and active context that the task is running on. 
        /// </summary>
        private readonly IDictionary<string, TaskInfo> _tasks
            = new Dictionary<string, TaskInfo>();

        /// <summary>
        /// This Dictionary keeps all the running tasks. The key is the Task Id and the value is IRunningTask. 
        /// After a task is running, it will be added to this collection. After the task is requested to close, 
        /// or fails, completed, it will be removed from this collection. 
        /// </summary>
        private readonly IDictionary<string, IRunningTask> _runningTasks = new Dictionary<string, IRunningTask>();

        /// <summary>
        /// This is the reference of the IGroupCommDriver. The IGroupCommDriver is injected with the driver constructor
        /// and passed to this class. 
        /// </summary>
        private readonly IGroupCommDriver _groupCommDriver;

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
        private int _numberOfAppError = 0;

        /// <summary>
        /// Creates a TaskManager with specified total number of tasks, master task id and associated IGroupCommDriver.
        /// Throws IMRUSystemException if numTasks is 0, or masterTaskId is null, or groupCommDriver is null.
        /// </summary>
        /// <param name="numTasks"></param>
        /// <param name="masterTaskId"></param>
        /// <param name="groupCommDriver"></param>
        internal TaskManager(int numTasks, string masterTaskId, IGroupCommDriver groupCommDriver)
        {
            if (numTasks <= 0)
            {
                Exceptions.Throw(new IMRUSystemException("Number of expected task cannot be 0"), Logger);
            }

            if (string.IsNullOrWhiteSpace(masterTaskId))
            {
                Exceptions.Throw(new IMRUSystemException("masterTaskId cannot be null"), Logger);
            }

            if (groupCommDriver == null)
            {
                Exceptions.Throw(new IMRUSystemException("groupCommDriver cannot be null"), Logger);
            }

            _totalExpectedTasks = numTasks;
            _masterTaskId = masterTaskId;
            _groupCommDriver = groupCommDriver;
        }

        /// <summary>
        /// Adds a Task to the task collection
        /// Throws IMRUSystemException in the following cases:
        ///   taskId is already added 
        ///   taskConfiguration is null
        ///   activeContext is null
        ///   trying to add extra tasks
        ///   trying to add Master Task twice
        ///   No Master Task is added in the collection
        /// </summary>
        /// <param name="taskId"></param>
        /// <param name="taskConfiguration"></param>
        /// <param name="activeContext"></param>
        internal void AddTask(string taskId, IConfiguration taskConfiguration, IActiveContext activeContext)
        {
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

            if (taskId.Equals(_masterTaskId) && MasterTaskExists())
            {
                string msg = string.Format("Trying to add second master Task {0}.", taskId);
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
        /// This method is called when receiving IRunningTask event during system SubmittingTasks state.
        /// Adds the IRunningTask to the running tasks collection and update the task state to TaskRunning.
        /// Throws IMRUSystemException if running tasks already contains this task or tasks collection doesn't contain this task.
        /// </summary>
        /// <param name="runningTask"></param>
        internal void SetRunningTask(IRunningTask runningTask)
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
            _numberOfAppError = 0;
        }

        /// <summary>
        /// This method is called when receiving ICompletedTask event during task running or system shutting down.
        /// Removes the task from running tasks
        /// Changes the task state from RunningTask to CompletedTask
        /// </summary>
        /// <param name="taskId"></param>
        internal void SetCompletedTask(string taskId)
        {
            RemoveRunningTask(taskId);
            UpdateState(taskId, TaskStateEvent.CompletedTask);
        }

        /// <summary>
        /// This method is called when receiving IFailedTask event during task submitting or running
        /// Removes the task from running tasks if the task was running
        /// Updates the task state to fail based on the error message in the failed task
        /// </summary>
        /// <param name="failedTask"></param>
        internal void SetFailedRunningTask(IFailedTask failedTask)
        {
            RemoveRunningTask(failedTask.Id);
            UpdateState(failedTask.Id, GetTaskErrorEvent(failedTask));
        }

        /// <summary>
        /// This method is called when receiving IFailedTask event during system shutting down. 
        /// If the task failed because it receives the close command from driver, update the task state to TaskClosedByDriver.
        /// Task could fail by communication error or any other application or system error during this time, as long as it is not 
        /// TaskFailedByEvaluatorFailure, update the task state based on the error received. 
        /// </summary>
        /// <param name="failedTask"></param>
        internal void SetFailedTaskInShuttingDown(IFailedTask failedTask)
        {
            if (TaskState(failedTask.Id) == StateMachine.TaskState.TaskWaitingForClose)
            {
                UpdateState(failedTask.Id, TaskStateEvent.ClosedTask);
            }
            else if (TaskState(failedTask.Id) != StateMachine.TaskState.TaskFailedByEvaluatorFailure)
            {
                UpdateState(failedTask.Id, GetTaskErrorEvent(failedTask));
            }
        }

        /// <summary>
        /// This method is called when receiving an IFailedEvaluator event during TaskSubmitted, TaskRunning or system ShuttingDown.
        /// Removes the task from RunningTasks if the task associated with the FailedEvaluator is present and running. 
        /// Sets the task state to TaskFailedByEvaluatorFailure 
        /// </summary>
        /// <param name="failedEvaluator"></param>
        internal void SetTaskFailByEvaluator(IFailedEvaluator failedEvaluator)
        {
            if (failedEvaluator.FailedTask.IsPresent())
            {
                var taskId = failedEvaluator.FailedTask.Value.Id;
                if (TaskState(taskId) == StateMachine.TaskState.TaskRunning)
                {
                    RemoveRunningTask(taskId);
                }

                if (TaskState(taskId) == StateMachine.TaskState.TaskWaitingForClose)
                {
                    UpdateState(taskId, TaskStateEvent.ClosedTask);
                }
                else
                {
                    UpdateState(taskId, TaskStateEvent.FailedTaskEvaluatorError);
                }
            }
        }

        /// <summary>
        /// Removes a task from running tasks if it exists in the running tasks collection
        /// </summary>
        /// <param name="taskId"></param>
        private void RemoveRunningTask(string taskId)
        {
            if (_runningTasks.ContainsKey(taskId))
            {
                _runningTasks.Remove(taskId);
            }
        }

        /// <summary>
        /// Updates task state for a given taskId based on the task event
        /// </summary>
        /// <param name="taskId"></param>
        /// <param name="taskEvent"></param>
        private void UpdateState(string taskId, TaskStateEvent taskEvent)
        {
            if (!_tasks.ContainsKey(taskId))
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "The task [{0}] doesn't exist.", taskId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }
            GetTaskInfo(taskId).TaskState.MoveNext(taskEvent);
        }

        /// <summary>
        /// Checks if all the tasks are running.
        /// </summary>
        /// <returns></returns>
        internal bool AreAllTasksRunning()
        {
            return _tasks.All(t => t.Value.TaskState.CurrentState == StateMachine.TaskState.TaskRunning) &&
                _runningTasks.Count == _totalExpectedTasks;
        }

        /// <summary>
        /// Checks if all the tasks are completed.
        /// </summary>
        /// <returns></returns>
        internal bool AreAllTasksAreCompleted()
        {
            return AreAllTasksInState(StateMachine.TaskState.TaskCompleted) && _tasks.Count == _totalExpectedTasks && _runningTasks.Count == 0;
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
            Logger.Log(Level.Verbose, string.Format(CultureInfo.InvariantCulture, "Closing [{0}] running tasks.", _runningTasks.Count));
            foreach (var runningTask in _runningTasks.Values)
            {
                runningTask.Dispose(Encoding.UTF8.GetBytes(closeMessage));
                UpdateState(runningTask.Id, TaskStateEvent.WaitingTaskToClose);
            }
            _runningTasks.Clear();
        }

        /// <summary>
        /// This method is called when receiving an IRunningTask event but system is either in ShuttingDown or Fail state.
        /// In this case, the task should not be added in Running Tasks yet.
        /// Change the task state to TaskRunning if it is still in TaskSubmitted state
        /// Closes the IRunningTask 
        /// Then move the task state to WaitingTaskToClose
        /// Throw IMRUSystemException if runningTask is null or the running task is already added in the running task collection
        /// </summary>
        /// <param name="runningTask"></param>
        /// <param name="closeMessage"></param>
        internal void CloseRunningTaskInSystemFailure(IRunningTask runningTask, string closeMessage)
        {
            if (runningTask == null)
            {
                Exceptions.Throw(new IMRUSystemException("RunningTask is null."), Logger);
            }
            else if (_runningTasks.ContainsKey(runningTask.Id))
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "The task [{0}] is already in running tasks.", runningTask.Id);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }
            else
            {
                if (TaskState(runningTask.Id) == StateMachine.TaskState.TaskSubmitted)
                {
                    UpdateState(runningTask.Id, TaskStateEvent.RunningTask);
                }
                runningTask.Dispose(Encoding.UTF8.GetBytes(closeMessage));
                UpdateState(runningTask.Id, TaskStateEvent.WaitingTaskToClose);
            }
        }

        /// <summary>
        /// Gets error type based on the information in IFailedTask 
        /// Currently we use the Message in IFailedTask to distinguish different types of errors
        /// </summary>
        /// <param name="failedTask"></param>
        /// <returns></returns>
        private TaskStateEvent GetTaskErrorEvent(IFailedTask failedTask)
        {
            var errorMessage = failedTask.Message;
            switch (errorMessage)
            {
                case TaskAppError:
                    _numberOfAppError++;
                    return TaskStateEvent.FailedTaskAppError;
                case TaskSystemError:
                    return TaskStateEvent.FailedTaskSystemError;
                case TaskGroupCommunicationError:
                    return TaskStateEvent.FailedTaskCommunicationError;
                default:
                    return TaskStateEvent.FailedTaskSystemError;
            }
        }

        /// <summary>
        /// Returns the number of application error caused by FailedTask
        /// </summary>
        /// <returns></returns>
        internal int NumberOfAppError()
        {
            return _numberOfAppError;
        }

        /// <summary>
        /// Checks if all the tasks are in final states
        /// </summary>
        /// <returns></returns>
        internal bool AllInFinalState()
        {
            return _tasks.All(t => t.Value.TaskState.IsFinalState());
        }

        /// <summary>
        /// Gets current state of the task
        /// </summary>
        /// <param name="taskId"></param>
        /// <returns></returns>
        internal TaskState TaskState(string taskId)
        {
            if (!_tasks.ContainsKey(taskId))
            {
                var msg = string.Format(CultureInfo.InvariantCulture, "The task [{0}] doesn't exist.", taskId);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

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
            if (NumberOfTasks < _totalExpectedTasks || !MasterTaskExists())
            {
                string msg = string.Format("Trying to submit tasks but either master task doesn't exist or number of tasks [{0}] is smaller than expected number of tasks [{1}].", NumberOfTasks, _totalExpectedTasks);
                Exceptions.Throw(new IMRUSystemException(msg), Logger);
            }

            StartTask(_masterTaskId);

            foreach (var taskId in _tasks.Keys)
            {
                if (taskId.Equals(_masterTaskId))
                {
                    continue;
                }
                StartTask(taskId);
            }
        }

        /// <summary>
        /// Starts a task and then update the task status to submitted.
        /// </summary>
        /// <param name="taskId"></param>
        private void StartTask(string taskId)
        {
            var taskInfo = GetTaskInfo(taskId);
            StartTask(taskId, taskInfo.TaskConfiguration, taskInfo.ActiveContext);
            taskInfo.TaskState.MoveNext(TaskStateEvent.SubmittedTask);
        }

        /// <summary>
        /// Gets GroupCommTaskConfiguration from IGroupCommDriver and merges it with the user partial task configuration.
        /// Then submits the task with the ActiveContext.
        /// </summary>
        /// <param name="taskId"></param>
        /// <param name="userPartialTaskConf"></param>
        /// <param name="activeContext"></param>
        private void StartTask(
            string taskId,
            IConfiguration userPartialTaskConf,
            ITaskSubmittable activeContext)
        {
            var groupCommTaskConfiguration = _groupCommDriver.GetGroupCommTaskConfiguration(taskId);
            var mergedTaskConf = Configurations.Merge(userPartialTaskConf, groupCommTaskConfiguration);
            activeContext.SubmitTask(mergedTaskConf);
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