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
using System.Linq;
using System.Threading;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Comm;
using System.Collections.Concurrent;
using Org.Apache.REEF.Wake.Time.Event;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Network.Elastic.Failures.Default;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Driver.Default
{
    /// <summary>
    /// Class managing the scheduling of tasks and task-related events.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal sealed class DefaultElasticTaskSetManager :
        IElasticTaskSetManager,
        IDefaultFailureEventResponse,
        IObserver<Alarm>
    {
        #region Private structs

        // Struct managing state for re-scheduling contexts after evaluator failures.
        private struct ContextInfo
        {
            public ContextInfo(int id)
            {
                Id = id;
                NumRetry = 1;
            }

            /// <summary>
            /// The context id.
            /// </summary>
            public int Id { get; private set; }

            /// <summary>
            /// The number of times we tried to submit the context.
            /// </summary>
            public int NumRetry { get; set; }
        }

        /// <summary>
        /// Definition of the the different states in which a task can be.
        /// </summary>
        private enum TaskState
        {
            Init = 1,

            Queued = 2,

            Submitted = 3,

            Recovering = 4,

            Running = 5,

            Failed = 6,

            Completed = 7
        }

        #endregion Private structs

        #region Private classes

        /// <summary>
        /// Wraps all the info required to proper manage a task life cycle.
        /// </summary>
        private sealed class TaskInfo : IDisposable
        {
            private volatile bool _isTaskDisposed = false;
            private volatile bool _isActiveContextDisposed = false;
            private volatile bool _isDisposed = false;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="config">The inital configuration for the task</param>
            /// <param name="context">The active context for the task</param>
            /// <param name="evaluatorId">The evalutor id</param>
            /// <param name="status">The task status</param>
            /// <param name="stages">The stage the task belongs to</param>
            public TaskInfo(
                IConfiguration config,
                IActiveContext context,
                string evaluatorId,
                TaskState status,
                IList<IElasticStage> stages)
            {
                TaskConfiguration = config;
                ActiveContext = context;
                EvaluatorId = evaluatorId;
                Stages = stages;
                NumRetry = 1;
                TaskStatus = status;
                RescheduleConfigurations = new Dictionary<string, IList<IConfiguration>>();
                Lock = new object();
            }

            /// <summary>
            /// The task configuration.
            /// </summary>
            public IConfiguration TaskConfiguration { get; private set; }

            /// <summary>
            /// The active context for the task.
            /// </summary>
            public IActiveContext ActiveContext { get; private set; }

            /// <summary>
            /// Whether the active task context was previously diposed or not.
            /// </summary>
            public bool IsActiveContextDisposed
            {
                get { return _isActiveContextDisposed; }
            }

            /// <summary>
            /// The id of the evalutor of the task.
            /// </summary>
            public string EvaluatorId { get; private set; }

            /// <summary>
            /// The stages the task will be exeucting.
            /// </summary>
            public IList<IElasticStage> Stages { get; private set; }

            /// <summary>
            /// Configurations when the task will be rescheduled after a failure.
            /// </summary>
            public Dictionary<string, IList<IConfiguration>> RescheduleConfigurations { get; set; }

            /// <summary>
            /// Reference to the remote running task.
            /// </summary>
            public IRunningTask TaskRunner { get; private set; }

            /// <summary>
            /// The current status of the task.
            /// </summary>
            public TaskState TaskStatus { get; private set; }

            /// <summary>
            /// How many times the task have been scheduled.
            /// </summary>
            public int NumRetry { get; set; }

            /// <summary>
            ///An object used as lock for the task info.
            /// </summary>
            public object Lock { get; private set; }

            /// <summary>
            /// Save the reference to the remote running task.
            /// </summary>
            /// <param name="taskRunner">The reference to the remote running task</param>
            public void SetTaskRunner(IRunningTask taskRunner)
            {
                TaskRunner = taskRunner;
                _isTaskDisposed = false;
            }

            /// <summary>
            /// Change the status of the task.
            /// </summary>
            /// <param name="status">The new task state</param>
            public void SetTaskStatus(TaskState status)
            {
                TaskStatus = status;
            }

            /// <summary>
            /// Update the task runtime.
            /// </summary>
            /// <param name="newActiveContext">The active context of the task</param>
            /// <param name="evaluatorId">The id of the evaluator</param>
            public void UpdateRuntime(IActiveContext newActiveContext, string evaluatorId)
            {
                if (!_isActiveContextDisposed)
                {
                    throw new IllegalStateException("Updating Task with not disposed active context");
                }

                ActiveContext = newActiveContext;
                EvaluatorId = evaluatorId;
                _isActiveContextDisposed = false;
            }

            /// <summary>
            /// Set the task runtime as diposed.
            /// </summary>
            public void DropRuntime()
            {
                _isActiveContextDisposed = true;
                _isTaskDisposed = true;
            }

            /// <summary>
            /// Dipose the task.
            /// </summary>
            public void DisposeTask()
            {
                if (!_isTaskDisposed)
                {
                    if (TaskRunner != null)
                    {
                        TaskRunner.Dispose();
                    }

                    _isTaskDisposed = true;
                }
            }

            /// <summary>
            /// Dipose the active context of the task.
            /// </summary>
            public void DisposeActiveContext()
            {
                if (!_isActiveContextDisposed)
                {
                    if (ActiveContext != null)
                    {
                        ActiveContext.Dispose();
                    }

                    _isActiveContextDisposed = true;
                }
            }

            /// <summary>
            /// Dipose the task info.
            /// </summary>
            public void Dispose()
            {
                if (!_isDisposed)
                {
                    DisposeTask();

                    DisposeActiveContext();

                    _isDisposed = true;
                }
            }
        }

        /// <summary>
        /// Utility class used to recognize particular task states.
        /// </summary>
        private static class TaskStateUtils
        {
            private static List<TaskState> recoverable = new List<TaskState>()
            {
                TaskState.Failed, TaskState.Queued
            };

            private static List<TaskState> notRunnable = new List<TaskState>()
            {
                TaskState.Failed, TaskState.Completed
            };

            /// <summary>
            /// Whether a task is recoverable or not.
            /// </summary>
            /// <param name="state">The current state of the task</param>
            /// <returns>True if the task is recoverable</returns>
            public static bool IsRecoverable(TaskState state)
            {
                return recoverable.Contains(state);
            }

            /// <summary>
            /// Whether a task can be run or not.
            /// </summary>
            /// <param name="state">The current state of the task</param>
            /// <returns>True if the task can be run</returns>
            public static bool IsRunnable(TaskState state)
            {
                return !notRunnable.Contains(state);
            }
        }

        /// <summary>
        /// Represent an event triggered by some timeout registered by the task set.
        /// </summary>
        private sealed class TasksetAlarm : Alarm
        {
            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="timestamp">The timestamp when the alarm should be triggered</param>
            /// <param name="handler">The handler of the event triggered by the alarm</param>
            public TasksetAlarm(long timestamp, IObserver<Alarm> handler) : base(timestamp, handler)
            {
            }
        }

        /// <summary>
        /// Class used to define a timeout on the task set triggering an alarm.
        /// </summary>
        private sealed class TaskSetTimeout : ITimeout
        {
            private readonly IObserver<Alarm> _handler;
            private readonly long _offset;
            private readonly string _id;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="offset">The offset used to define when the timeout will be triggered</param>
            /// <param name="handler">The handler for the alarm</param>
            public TaskSetTimeout(long offset, IObserver<Alarm> handler)
            {
                _handler = handler ?? throw new ArgumentNullException(nameof(handler));
                _offset = offset;
            }

            /// <summary>
            /// Get the actual alarm to be scheduled.
            /// </summary>
            /// <param name="time">The current time</param>
            /// <returns></returns>
            public Alarm GetAlarm(long time)
            {
                return new TasksetAlarm(time + _offset, _handler);
            }
        }

        #endregion Private classes

        private static readonly Logger Log = Logger.GetLogger(typeof(DefaultElasticTaskSetManager));

        private bool _finalized = false;
        private volatile bool _disposed = false;
        private volatile bool _scheduled = false;
        private volatile bool _completed = false;
        private readonly DefaultElasticTaskSetManagerParameters _parameters;

        private volatile int _contextsAdded = 0;
        private int _tasksAdded = 0;
        private int _tasksRunning = 0;
        private volatile int _totFailedTasks = 0;
        private volatile int _totFailedEvaluators = 0;

        private readonly int _numTasks;
        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private readonly string _driverId;
        private readonly TaskConfigurator _masterTaskConfiguration;
        private readonly TaskConfigurator _slaveTaskConfiguration;

        // Task info 0-indexed
        private readonly List<TaskInfo> _taskInfos;

        private readonly Dictionary<string, IElasticStage> _stages = new Dictionary<string, IElasticStage>();
        private readonly ConcurrentQueue<int> _queuedTasks = new ConcurrentQueue<int>();
        private readonly ConcurrentQueue<ContextInfo> _queuedContexts = new ConcurrentQueue<ContextInfo>();

        // Used both for knowing which evaluator the task set is responsible for and to
        // maintain a mapping betwween evaluators and contextes.
        // This latter is necessary because evaluators may fail between context init
        // and the time when the context is installed on the evaluator
        private readonly ConcurrentDictionary<string, ContextInfo> _evaluatorToContextIdMapping =
            new ConcurrentDictionary<string, ContextInfo>();

        private IFailureState _failureStatus = new DefaultFailureState();
        private volatile bool _hasProgress = true;

        private readonly object _statusLock = new object();

        /// <summary>
        /// Constructor for the task set manager.
        /// </summary>
        /// <param name="numTasks">The total number of tasks in the task set</param>
        /// <param name="evaluatorRequestor">The requestor to spawn new evaluator</param>
        /// <param name="driverId">The id of the dirver</param>
        /// <param name="masterTaskConfiguration">The configuration for the master task</param>
        /// <param name="slaveTaskConfiguration">The configuration for the slave tasks</param>
        /// <param name="confs">Additional configurations</param>
        public DefaultElasticTaskSetManager(
            int numTasks,
            IEvaluatorRequestor evaluatorRequestor,
            string driverId,
            TaskConfigurator masterTaskConfiguration,
            TaskConfigurator slaveTaskConfiguration = null,
            params IConfiguration[] confs)
        {
            _numTasks = numTasks;
            _evaluatorRequestor = evaluatorRequestor;
            _driverId = driverId;
            _masterTaskConfiguration = masterTaskConfiguration;
            _slaveTaskConfiguration = slaveTaskConfiguration ?? masterTaskConfiguration;

            _taskInfos = new List<TaskInfo>(numTasks);

            for (int i = 0; i < numTasks; i++)
            {
                _taskInfos.Add(null);
            }

            var injector = TangFactory.GetTang().NewInjector(confs);
            Type parametersType = typeof(DefaultElasticTaskSetManagerParameters);
            _parameters = injector.GetInstance(parametersType) as DefaultElasticTaskSetManagerParameters;

            // Set up the timeout
            List<IElasticDriverMessage> msgs = null;
            var nextTimeouts = new List<ITimeout>();

            OnTimeout(new TasksetAlarm(0, this), ref msgs, ref nextTimeouts);
        }

        /// <summary>
        /// An identifier for the set of stages the task manager is subscribed to.
        /// The task set has to be built before retrieving its stages id.
        /// </summary>
        public string StagesId
        {
            get
            {
                if (_finalized != true)
                {
                    throw new IllegalStateException("Task set have to be built before getting its stages");
                }

                return _stages.Keys.Aggregate((current, next) => current + "+" + next);
            }
        }

        /// <summary>
        /// Subscribe the current task set manager to a new stage.
        /// </summary>
        /// <param name="stage">The stage to subscribe to</param>
        /// <returns>The same finalized task set manager</returns>
        public IElasticTaskSetManager AddStage(IElasticStage stage)
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("Cannot add stage to an already built task set manager");
            }

            _stages.Add(stage.StageName, stage);

            return this;
        }

        /// <summary>
        /// Decides whether more contexts have to be added to this Task Manger or not.
        /// </summary>
        /// <returns>True if the number of added contexts is less than the available slots</returns>

        public bool HasMoreContextToAdd()
        {
            return _contextsAdded < _numTasks;
        }

        /// <summary>
        /// Method used to generate unique context ids.
        /// </summary>
        /// <param name="evaluator">The evaluator the context will run on</param>
        /// <param name="identifier">A new unique context id</param>
        /// <returns>True if an new context id is sucessufully created</returns>
        public bool TryGetNextTaskContextId(IAllocatedEvaluator evaluator, out string identifier)
        {
            int id;
            ContextInfo cinfo;

            if (_queuedTasks.TryDequeue(out id))
            {
                identifier = Utils.BuildContextId(StagesId, id);
                cinfo = new ContextInfo(id);
                _evaluatorToContextIdMapping.TryAdd(evaluator.Id, cinfo);
                return true;
            }

            if (_queuedContexts.TryDequeue(out cinfo))
            {
                identifier = Utils.BuildContextId(StagesId, cinfo.Id);
                _evaluatorToContextIdMapping.TryAdd(evaluator.Id, cinfo);
                return true;
            }

            id = Interlocked.Increment(ref _contextsAdded);

            if (_contextsAdded > _numTasks)
            {
                Log.Log(Level.Warning, "Trying to schedule too many contexts");
                identifier = string.Empty;
                return false;
            }

            identifier = Utils.BuildContextId(StagesId, id);
            cinfo = new ContextInfo(id);
            _evaluatorToContextIdMapping.TryAdd(evaluator.Id, cinfo);

            Log.Log(Level.Info, "Evaluator {0} is scheduled on node {1}",
                evaluator.Id,
                evaluator.GetEvaluatorDescriptor().NodeDescriptor.HostName);

            return true;
        }

        /// <summary>
        /// Method used to generate unique task ids.
        /// </summary>
        /// <param name="context">The context the task will run on</param>
        /// <returns>A new task id</returns>
        public string GetTaskId(IActiveContext context)
        {
            var id = Utils.GetContextNum(context);
            return Utils.BuildTaskId(StagesId, id);
        }

        /// <summary>
        /// Retrieve all stages having the context passed as a parameter as master task context.
        /// </summary>
        /// <param name="context">The target context</param>
        /// <returns>A list of stages having the master task running on context</returns>
        public IEnumerable<IElasticStage> IsMasterTaskContext(IActiveContext activeContext)
        {
            return _stages.Values.Where(stage => stage.IsMasterTaskContext(activeContext));
        }

        /// <summary>
        /// Get the configuration of the codecs used for data transmission.
        /// The codecs are automatically generated from the operator pipeline.
        /// </summary>
        /// <returns>A configuration object with the codecs for data transmission</returns>
        public IConfiguration GetCodecConfiguration()
        {
            var conf = TangFactory.GetTang().NewConfigurationBuilder().Build();

            foreach (var stage in _stages.Values)
            {
                stage.PipelineRoot.GetCodecConfiguration(ref conf);
            }

            return conf;
        }

        /// <summary>
        /// Method implementing how the task set manager should react when a new context is active.
        /// </summary>
        /// <param name="activeContext">The new active context</param>
        public void OnNewActiveContext(IActiveContext activeContext)
        {
            if (_finalized != true)
            {
                throw new IllegalStateException("Task set have to be finalized before adding tasks.");
            }

            if (Completed() || Failed())
            {
                Log.Log(Level.Warning, "Adding tasks to already completed task set: ignoring.");
                activeContext.Dispose();
                return;
            }

            _hasProgress = true;
            var id = Utils.GetContextNum(activeContext) - 1;
            var taskId = Utils.BuildTaskId(StagesId, id + 1);

            // We reschedule the task only if the context was active (_taskInfos[id] != null) and the task was
            // actually scheduled at least once (_taskInfos[id].TaskStatus > TaskStatus.Init)
            if (_taskInfos[id] != null && _taskInfos[id].TaskStatus > TaskState.Init)
            {
                Log.Log(Level.Info, "{0} already part of task set: going to directly submit it.", taskId);

                lock (_taskInfos[id].Lock)
                {
                    _taskInfos[id].UpdateRuntime(activeContext, activeContext.EvaluatorId);
                }

                SubmitTask(id);
            }
            else
            {
                bool isMaster = IsMasterTaskContext(activeContext).Any();

                Log.Log(Level.Info, "Task {0} to be scheduled on {1}", taskId, activeContext.EvaluatorId);

                List<IConfiguration> partialTaskConfs = new List<IConfiguration>();

                if (isMaster)
                {
                    partialTaskConfs.Add(_masterTaskConfiguration(taskId));
                }
                else
                {
                    partialTaskConfs.Add(_slaveTaskConfiguration(taskId));
                }

                AddTask(taskId, activeContext, partialTaskConfs);
            }
        }

        /// <summary>
        /// Finalizes the task set manager.
        /// After the task set has been finalized, no more stages can be added.
        /// </summary>
        /// <returns>The same finalized task set manager</returns>
        public IElasticTaskSetManager Build()
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("Task set manager cannot be built more than once");
            }

            _finalized = true;

            return this;
        }

        /// <summary>
        /// Method implementing how the task set manager should react when a notification that a task is
        /// running is received.
        /// </summary>
        /// <param name="task">The running task</param>
        public void OnTaskRunning(IRunningTask task)
        {
            if (IsTaskManagedBy(task.Id))
            {
                var id = Utils.GetTaskNum(task.Id) - 1;
                _hasProgress = true;

                lock (_taskInfos[id].Lock)
                {
                    _taskInfos[id].SetTaskRunner(task);

                    if (Completed() || Failed())
                    {
                        Log.Log(Level.Info, "Received running from task {0} but task set is completed "
                            + "or failed: ignoring.", task.Id);
                        _taskInfos[id].Dispose();

                        return;
                    }
                    if (!TaskStateUtils.IsRunnable(_taskInfos[id].TaskStatus))
                    {
                        Log.Log(Level.Info, "Received running from task {0} which is not runnable: ignoring.",
                            task.Id);
                        _taskInfos[id].Dispose();

                        return;
                    }

                    if (_taskInfos[id].TaskStatus != TaskState.Running)
                    {
                        if (_taskInfos[id].TaskStatus == TaskState.Recovering)
                        {
                            foreach (var stage in _stages)
                            {
                                stage.Value.AddTask(task.Id);
                            }
                        }

                        _taskInfos[id].SetTaskStatus(TaskState.Running);
                        Interlocked.Increment(ref _tasksRunning);
                    }
                }
            }
        }

        /// <summary>
        /// Method implementing how the task set manager should react when a notification that a task
        /// is completed is received.
        /// </summary>
        /// <param name="task">The completed task</param>
        public void OnTaskCompleted(ICompletedTask taskInfo)
        {
            if (IsTaskManagedBy(taskInfo.Id))
            {
                Interlocked.Decrement(ref _tasksRunning);
                var id = Utils.GetTaskNum(taskInfo.Id) - 1;
                _hasProgress = true;

                lock (_taskInfos[id].Lock)
                {
                    _taskInfos[id].SetTaskStatus(TaskState.Completed);
                }
                if (Completed())
                {
                    foreach (var info in _taskInfos.Where(info => info != null && info.TaskStatus < TaskState.Failed))
                    {
                        info.DisposeTask();
                    }
                }
            }
        }

        /// <summary>
        /// Method implementing how the task set manager should react when a task message is received.
        /// </summary>
        /// <param name="message">A message from a task</param>
        public void OnTaskMessage(ITaskMessage message)
        {
            if (IsTaskManagedBy(message.TaskId))
            {
                var id = Utils.GetTaskNum(message.TaskId) - 1;
                var returnMessages = new List<IElasticDriverMessage>();
                _hasProgress = true;

                try
                {
                    foreach (var stage in _stages.Values)
                    {
                        stage.OnTaskMessage(message, ref returnMessages);
                    }
                }
                catch (IllegalStateException e)
                {
                    Log.Log(Level.Error, e.Message, e);
                    Fail(message.TaskId);
                }

                SendToTasks(returnMessages);
            }
        }

        /// <summary>
        /// Whether this task set is done.
        /// </summary>
        public bool IsCompleted()
        {
            return Completed() && _tasksRunning == 0;
        }

        #region Failure Response

        /// <summary>
        /// Used to react on a task failure.
        /// </summary>
        /// <param name="task">The failed task</param>
        public void OnTaskFailure(IFailedTask task)
        {
            var failureEvents = new List<IFailureEvent>();

            OnTaskFailure(task, ref failureEvents);
        }

        /// <summary>
        /// Used to react when a timeout event is triggered.
        /// </summary>
        /// <param name="alarm">The alarm triggering the timeput</param>
        /// <param name="msgs">A list of messages encoding how remote tasks need to react</param>
        /// <param name="nextTimeouts">The next timeouts to be scheduled</param>
        public void OnTimeout(Alarm alarm, ref List<IElasticDriverMessage> msgs, ref List<ITimeout> nextTimeouts)
        {
            var isInit = msgs == null;

            // Taskset is just started, init the timeouts
            if (isInit)
            {
                _hasProgress = false;
                Log.Log(Level.Info, "Timeout alarm for task set initialized");
                nextTimeouts.Add(new TaskSetTimeout(_parameters.Timeout, this));

                foreach (var stage in _stages.Values)
                {
                    stage.OnTimeout(alarm, ref msgs, ref nextTimeouts);
                }
            }
            else if (alarm.GetType() == typeof(TasksetAlarm))
            {
                if (!_hasProgress)
                {
                    if (Completed() || Failed())
                    {
                        Log.Log(Level.Warning, "Taskset made no progress in the last {0}ms. Forcing Disposal.",
                            _parameters.Timeout);
                        Dispose();
                    }
                    else
                    {
                        Log.Log(Level.Error, "Taskset made no progress in the last {0}ms. Aborting.",
                            _parameters.Timeout);
                        Fail();
                        return;
                    }
                }
                else
                {
                    _hasProgress = false;
                    nextTimeouts.Add(new TaskSetTimeout(_parameters.Timeout, this));
                }
            }
            else
            {
                foreach (var stage in _stages.Values)
                {
                    stage.OnTimeout(alarm, ref msgs, ref nextTimeouts);
                }

                SendToTasks(msgs);
            }

            foreach (var timeout in nextTimeouts)
            {
                _parameters.Clock.ScheduleAlarm(timeout);
            }
        }

        /// <summary>
        /// Used to react on a failure occurred on a task.
        /// It gets a failed task as input and in response it produces zero or more failure events.
        /// </summary>
        /// <param name="task">The failed task</param>
        /// <param name="failureEvents">A list of events encoding the type of actions to be triggered so far</param>
        /// <exception cref="Exception">If the task failure cannot be properly handled</exception>

        public void OnTaskFailure(IFailedTask task, ref List<IFailureEvent> failureEvents)
        {
            if (IsTaskManagedBy(task.Id))
            {
                Log.Log(Level.Info, "Received a failure from {0}", task.Id, task.AsError());

                Interlocked.Decrement(ref _tasksRunning);
                _totFailedTasks++;
                _hasProgress = true;
                var id = Utils.GetTaskNum(task.Id) - 1;

                if (Completed() || Failed())
                {
                    Log.Log(Level.Info, "Received a failure from task {0} but the task set is completed or "
                        + "failed: ignoring the failure", task.Id, task.AsError());

                    lock (_taskInfos[id].Lock)
                    {
                        _taskInfos[id].SetTaskStatus(TaskState.Failed);
                    }

                    _taskInfos[id].Dispose();

                    return;
                }

                failureEvents = failureEvents ?? new List<IFailureEvent>();

                lock (_taskInfos[id].Lock)
                {
                    if (_taskInfos[id].TaskStatus < TaskState.Failed)
                    {
                        _taskInfos[id].SetTaskStatus(TaskState.Failed);
                    }

                    try
                    {
                        foreach (var stage in _taskInfos[id].Stages)
                        {
                            stage.OnTaskFailure(task, ref failureEvents);
                        }
                    }
                    catch (Exception e)
                    {
                        Log.Log(Level.Error, e.Message, e);
                        Fail(task.Id);
                    }

                    // Failures have to be propagated up to the context
                    _taskInfos[id].Stages.First().Context.OnTaskFailure(task, ref failureEvents);
                }

                for (int i = 0; i < failureEvents.Count; i++)
                {
                    var @event = failureEvents[i];
                    EventDispatcher(ref @event);
                }
            }
        }

        /// <summary>
        /// Used to react of a failure event occurred on an evaluator.
        /// </summary>
        /// <param name="evaluator">The failed evaluator</param>
        public void OnEvaluatorFailure(IFailedEvaluator evaluator)
        {
            Log.Log(Level.Info, "Received a failure from {0}", evaluator.Id, evaluator.EvaluatorException);

            _totFailedEvaluators++;

            if (evaluator.FailedTask.IsPresent())
            {
                var failedTask = evaluator.FailedTask.Value;
                var id = Utils.GetTaskNum(failedTask.Id) - 1;

                lock (_taskInfos[id].Lock)
                {
                    _taskInfos[id].DropRuntime();
                }

                OnTaskFailure(failedTask);
                _evaluatorToContextIdMapping.TryRemove(evaluator.Id, out ContextInfo cinfo);
            }
            else
            {
                _hasProgress = true;

                if (!Completed() && !Failed())
                {
                    if (_evaluatorToContextIdMapping.TryRemove(evaluator.Id, out ContextInfo cinfo))
                    {
                        int id = cinfo.Id - 1;

                        if (_taskInfos[id] != null)
                        {
                            lock (_taskInfos[id].Lock)
                            {
                                _taskInfos[id].DropRuntime();
                                _taskInfos[id].SetTaskStatus(TaskState.Failed);
                            }
                        }

                        cinfo.NumRetry++;

                        if (cinfo.NumRetry > _parameters.NumEvaluatorFailures)
                        {
                            Log.Log(Level.Error, "Context {0} failed more than {1} times: Aborting",
                                cinfo.Id,
                                _parameters.NumEvaluatorFailures);
                            Fail();
                        }

                        _queuedContexts.Enqueue(cinfo);
                    }
                    SpawnNewEvaluator(cinfo.Id);
                }
            }
        }

        /// <summary>
        /// When a new failure state is reached, this method is used to dispatch
        /// such event to the proper failure mitigation logic.
        /// It gets a failure event as input and produces zero or more failure response messages
        /// for tasks (appended into the event).
        /// </summary>
        /// <param name="event">The failure event to react upon</param>
        public void EventDispatcher(ref IFailureEvent @event)
        {
            var id = Utils.GetTaskNum(@event.TaskId) - 1;

            _taskInfos[id].Stages.First().Context.EventDispatcher(ref @event);

            foreach (var stage in _taskInfos[id].Stages)
            {
                stage.EventDispatcher(ref @event);
            }

            switch ((DefaultFailureStateEvents)@event.FailureEvent)
            {
                case DefaultFailureStateEvents.Reconfigure:
                    var rec = @event as ReconfigureEvent;
                    OnReconfigure(ref rec);
                    break;

                case DefaultFailureStateEvents.Reschedule:
                    var res = @event as RescheduleEvent;
                    OnReschedule(ref res);
                    break;

                case DefaultFailureStateEvents.Stop:
                    var stp = @event as StopEvent;
                    OnStop(ref stp);
                    break;

                case DefaultFailureStateEvents.Fail:
                    OnFail();
                    break;

                default:
                    throw new IllegalStateException("Failure event not recognized.");
            }
        }

        /// <summary>
        /// Mechanism to execute when a reconfigure event is triggered.
        /// <paramref name="reconfigureEvent"/>
        /// </summary>
        public void OnReconfigure(ref ReconfigureEvent reconfigureEvent)
        {
            lock (_statusLock)
            {
                _failureStatus = _failureStatus.Merge(
                    new DefaultFailureState((int)DefaultFailureStates.ContinueAndReconfigure));
            }

            SendToTasks(reconfigureEvent.FailureResponse);
        }

        /// <summary>
        /// Mechanism to execute when a reschedule event is triggered.
        /// <paramref name="rescheduleEvent"/>
        /// </summary>
        public void OnReschedule(ref RescheduleEvent rescheduleEvent)
        {
            lock (_statusLock)
            {
                _failureStatus = _failureStatus.Merge(
                    new DefaultFailureState((int)DefaultFailureStates.ContinueAndReschedule));
            }

            SendToTasks(rescheduleEvent.FailureResponse);

            Reschedule(rescheduleEvent);
        }

        /// <summary>
        /// Mechanism to execute when a stop event is triggered.
        /// <paramref name="stopEvent"/>
        /// </summary>
        public void OnStop(ref StopEvent stopEvent)
        {
            lock (_statusLock)
            {
                _failureStatus = _failureStatus.Merge(
                    new DefaultFailureState((int)DefaultFailureStates.StopAndReschedule));
            }

            SendToTasks(stopEvent.FailureResponse);

            var rescheduleEvent = stopEvent as RescheduleEvent;

            Reschedule(rescheduleEvent);
        }

        /// <summary>
        /// Mechanism to execute when a fail event is triggered.
        /// </summary>
        public void OnFail()
        {
            Log.Log(Level.Info, "Task set failed");

            lock (_statusLock)
            {
                _failureStatus = _failureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.Fail));
            }

            Dispose();
        }

        #endregion Failure Response

        public void Dispose()
        {
            if (!_disposed)
            {
                _disposed = true;
                LogFinalStatistics();

                foreach (var info in _taskInfos)
                {
                    if (info != null)
                    {
                        lock (info.Lock)
                        {
                            info.Dispose();
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Whether the imput task is managed by this task set manger.
        /// </summary>
        /// <param name="id">The task identifier</param>
        public bool IsTaskManagedBy(string id)
        {
            return Utils.GetTaskStages(id) == StagesId;
        }

        /// <summary>
        /// Whether the imput context is managed by this task set manger.
        /// </summary>
        /// <param name="id">The context identifier</param>
        public bool IsContextManagedBy(string id)
        {
            return Utils.GetContextStages(id) == StagesId;
        }

        /// <summary>
        /// Whether the imput evaluator is managed by this task set manger.
        /// </summary>
        /// <param name="id">The context identifier</param>
        public bool IsEvaluatorManagedBy(string id)
        {
            return _evaluatorToContextIdMapping.ContainsKey(id);
        }

        /// <summary>
        /// Observer reacting to an alarm event.
        /// </summary>
        /// <param name="alarm">The alarm</param>
        public void OnNext(Alarm alarm)
        {
            var msgs = new List<IElasticDriverMessage>();
            var nextTimeouts = new List<ITimeout>();

            OnTimeout(alarm, ref msgs, ref nextTimeouts);
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        private void AddTask(string taskId, IActiveContext activeContext, List<IConfiguration> partialTaskConfigs)
        {
            Interlocked.Increment(ref _tasksAdded);
            var stageList = new List<IElasticStage>();
            var id = Utils.GetTaskNum(taskId) - 1;

            foreach (var stage in _stages)
            {
                if (stage.Value.AddTask(taskId))
                {
                    stageList.Add(stage.Value);
                    var partitionConf = stage.Value.GetPartitionConf(taskId);

                    if (partitionConf.IsPresent())
                    {
                        partialTaskConfigs.Add(partitionConf.Value);
                    }
                }
                else
                {
                    Log.Log(Level.Warning, "{0} cannot be added to stage {1}", taskId, stage.Key);
                    activeContext.Dispose();
                    return;
                }
            }

            var aggregatedConfs = partialTaskConfigs.Aggregate((x, y) => Configurations.Merge(x, y));

            _taskInfos[id] = new TaskInfo(
                aggregatedConfs,
                activeContext,
                activeContext.EvaluatorId,
                TaskState.Init,
                stageList);

            if (_scheduled)
            {
                SubmitTask(id);
            }
            else if (StartSubmitTasks())
            {
                SubmitTasks();
            }
        }

        private bool StartSubmitTasks()
        {
            lock (_statusLock)
            {
                if (_scheduled)
                {
                    return false;
                }

                if (_stages.All(stage => stage.Value.ScheduleStage()))
                {
                    _scheduled = true;

                    Log.Log(Level.Info, "Scheduling {0} tasks from Taskset {1}", _tasksAdded, StagesId);
                }
            }

            return _scheduled;
        }

        private void SubmitTasks()
        {
            for (int i = 0; i < _numTasks; i++)
            {
                if (_taskInfos[i] != null)
                {
                    SubmitTask(i);
                }
            }
        }

        private void SubmitTask(int id)
        {
            if (Completed() || Failed())
            {
                Log.Log(Level.Warning, "Task submit for a completed or failed Task Set: ignoring");
                _taskInfos[id].DisposeTask();

                return;
            }

            lock (_taskInfos[id].Lock)
            {
                // Check that the task was not already submitted. This may happen for instance if
                // _scheduled is set to true and a new active context message is received.
                if (_taskInfos[id].TaskStatus == TaskState.Submitted)
                {
                    return;
                }

                var stages = _taskInfos[id].Stages;
                ICsConfigurationBuilder confBuilder = TangFactory.GetTang().NewConfigurationBuilder();
                var rescheduleConfs = _taskInfos[id].RescheduleConfigurations;

                foreach (var stage in stages)
                {
                    var confSub = stage.GetTaskConfiguration(id + 1);

                    if (rescheduleConfs.TryGetValue(stage.StageName, out var confs))
                    {
                        foreach (var additionalConf in confs)
                        {
                            confSub = Configurations.Merge(confSub, additionalConf);
                        }
                    }

                    _stages.Values.First().Context.SerializeStageConfiguration(ref confBuilder, confSub);
                }

                IConfiguration baseConf = confBuilder
                .BindNamedParameter<ElasticServiceConfigurationOptions.DriverId, string>(
                    GenericType<ElasticServiceConfigurationOptions.DriverId>.Class,
                    _driverId)
                .Build();

                IConfiguration mergedTaskConf = Configurations.Merge(_taskInfos[id].TaskConfiguration, baseConf);

                if (_taskInfos[id].IsActiveContextDisposed)
                {
                    Log.Log(Level.Warning,
                        "Task submit for {0} with a non-active context: spawning a new evaluator.", id + 1);

                    if (_taskInfos[id].TaskStatus == TaskState.Failed)
                    {
                        _queuedTasks.Enqueue(id + 1);
                        _taskInfos[id].SetTaskStatus(TaskState.Queued);

                        SpawnNewEvaluator(id);
                    }

                    return;
                }

                _taskInfos[id].ActiveContext.SubmitTask(mergedTaskConf);

                if (TaskStateUtils.IsRecoverable(_taskInfos[id].TaskStatus))
                {
                    _taskInfos[id].SetTaskStatus(TaskState.Recovering);
                }
                else
                {
                    _taskInfos[id].SetTaskStatus(TaskState.Submitted);
                }
            }
        }

        private void SendToTasks(IList<IElasticDriverMessage> messages, int retry = 0)
        {
            foreach (var returnMessage in messages)
            {
                if (returnMessage != null)
                {
                    var destination = Utils.GetTaskNum(returnMessage.Destination) - 1;

                    if (_taskInfos[destination] == null)
                    {
                        throw new ArgumentNullException("Task Info");
                    }
                    lock (_taskInfos[destination].Lock)
                    {
                        if (Completed() || Failed())
                        {
                            Log.Log(Level.Warning, "Task submit for a completed or failed Task Set: ignoring");
                            _taskInfos[destination].DisposeTask();

                            return;
                        }
                        if (_taskInfos[destination].TaskStatus != TaskState.Running ||
                            _taskInfos[destination].TaskRunner == null)
                        {
                            var msg = string.Format("Cannot send message to {0}:", destination + 1);
                            msg += ": Task Status is " + _taskInfos[destination].TaskStatus;

                            if (_taskInfos[destination].TaskStatus == TaskState.Submitted && retry < _parameters.Retry)
                            {
                                Log.Log(Level.Warning, msg + " Retry");
                                System.Threading.Tasks.Task.Run(() =>
                                {
                                    Thread.Sleep(_parameters.WaitTime);
                                    SendToTasks(new List<IElasticDriverMessage>() { returnMessage }, retry + 1);
                                });
                            }
                            else if (retry >= _parameters.Retry)
                            {
                                Log.Log(Level.Warning, msg + " Aborting");
                                Fail(returnMessage.Destination);
                            }
                            else
                            {
                                Log.Log(Level.Warning, msg + " Ignoring");
                            }

                            continue;
                        }

                        _taskInfos[destination].TaskRunner.Send(returnMessage.Serialize());
                    }
                }
            }
        }

        private void SpawnNewEvaluator(int id)
        {
            Log.Log(Level.Warning, "Spawning new evaluator for id {0}", id);

            var request = _evaluatorRequestor.NewBuilder()
                .SetNumber(1)
                .SetMegabytes(_parameters.NewEvaluatorMemorySize)
                .SetCores(_parameters.NewEvaluatorNumCores)
                .SetRackName(_parameters.NewEvaluatorRackName)
                .Build();

            _evaluatorRequestor.Submit(request);
        }

        private void Reschedule(RescheduleEvent rescheduleEvent)
        {
            var id = Utils.GetTaskNum(rescheduleEvent.TaskId) - 1;

            lock (_taskInfos[id].Lock)
            {
                _taskInfos[id].NumRetry++;

                if (_taskInfos[id].NumRetry > _parameters.NumTaskFailures)
                {
                    Log.Log(Level.Error, "Task {0} failed more than {1} times: aborting",
                        rescheduleEvent.TaskId,
                        _parameters.NumTaskFailures);
                    Fail(rescheduleEvent.TaskId);
                }

                if (rescheduleEvent.Reschedule)
                {
                    Log.Log(Level.Info, "Rescheduling task {0}", rescheduleEvent.TaskId);

                    _taskInfos[id].RescheduleConfigurations = rescheduleEvent.RescheduleTaskConfigurations;

                    SubmitTask(id);
                }
            }
        }

        private void Fail(string taskId = "")
        {
            IFailureEvent @event = new FailEvent(taskId);

            EventDispatcher(ref @event);
        }

        private void LogFinalStatistics()
        {
            var msg = string.Format("Total Failed Tasks: {0}\nTotal Failed Evaluators: {1}",
                _totFailedTasks,
                _totFailedEvaluators);
            msg += _stages.Select(x => x.Value.LogFinalStatistics()).Aggregate((a, b) => a + "\n" + b);
            Log.Log(Level.Info, msg);
        }

        private bool Completed()
        {
            if (!_completed)
            {
                _completed = _stages.Select(stage => stage.Value.IsCompleted).Aggregate((com1, com2) => com1 && com2);

                if (_completed)
                {
                    Log.Log(Level.Info, "Task set completed");
                }
            }

            return _completed;
        }

        private bool Failed()
        {
            return _failureStatus.FailureState == (int)DefaultFailureStates.Fail;
        }
    }
}