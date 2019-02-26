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

using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using System.Threading;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Network.Elastic.Failures;
using System.Collections.Generic;
using Org.Apache.REEF.Network.Elastic.Comm;
using System.Linq;
using Org.Apache.REEF.Wake.Time.Event;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Utilities;
using System;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Network.Elastic.Failures.Default;
using Org.Apache.REEF.Network.Elastic.Operators.Logical.Default;
using Org.Apache.REEF.Network.Elastic.Operators.Logical;
using Org.Apache.REEF.Tang.Implementations.Tang;

namespace Org.Apache.REEF.Network.Elastic.Driver.Default
{
    /// <summary>
    /// Used to group elastic operators into logical units.
    /// All operators in the same stages share similar semantics and behavior
    /// under failures. Stages can only be created by a service.
    /// This class is used to create stages able to manage default failure events.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal sealed class DefaultElasticStage : IElasticStage, IDefaultFailureEventResponse
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(DefaultElasticStage));

        private bool _finalized = false;
        private volatile bool _scheduled = false;

        private readonly int _numTasks;
        private int _tasksAdded = 0;
        private HashSet<string> _missingMasterTasks = new HashSet<string>();
        private HashSet<string> _masterTasks = new HashSet<string>();
        private readonly IFailureStateMachine _failureMachine;

        private int _numOperators;
        private Optional<IConfiguration[]> _datasetConfiguration;
        private bool _isMasterGettingInputData;

        private readonly object _tasksLock = new object();
        private readonly object _statusLock = new object();

        /// <summary>
        /// Create a new stage with the input settings.
        /// </summary>
        /// <param name="stageName">The name of the stage</param>
        /// <param name="numTasks">The number of tasks managed by the stage</param>
        /// <param name="elasticService">The service managing the stage</param>
        /// <param name="failureMachine">The failure machine for the stage</param>
        internal DefaultElasticStage(
            string stageName,
            int numTasks,
            IElasticContext elasticService,
            IFailureStateMachine failureMachine = null)
        {
            StageName = stageName;
            _numTasks = numTasks;
            _datasetConfiguration = Optional<IConfiguration[]>.Empty();
            Context = elasticService;
            _failureMachine = failureMachine ?? new DefaultFailureStateMachine(numTasks, DefaultFailureStates.Fail);
            FailureState = _failureMachine.State;
            PipelineRoot = new DefaultEmpty(this, _failureMachine.Clone());

            IsIterative = false;
        }

        /// <summary>
        /// The name of the stages.
        /// </summary>
        public string StageName { get; set; }

        /// <summary>
        /// The operator at the beginning of the computation workflow.
        /// </summary>
        public ElasticOperator PipelineRoot { get; private set; }

        /// <summary>
        /// The service managing the stages.
        /// </summary
        public IElasticContext Context { get; private set; }

        /// <summary>
        /// Whether the stages contains iterations or not.
        /// </summary>
        public bool IsIterative { get; set; }

        /// <summary>
        /// The failure state of the target stages.
        /// </summary>
        public IFailureState FailureState { get; private set; }

        /// <summary>
        /// Whether the stages is completed or not.
        /// </summary>
        public bool IsCompleted
        {
            get { return FailureState.FailureState.IsComplete(); }
        }

        /// <summary>
        /// Generates an id to uniquely identify operators in the stages.
        /// </summary>
        /// <returns>A new unique id</returns>
        public int GetNextOperatorId()
        {
            return Interlocked.Increment(ref _numOperators);
        }

        /// <summary>
        /// Add a partitioned dataset to the stage.
        /// </summary>
        /// <param name="inputDataSet">The partitioned dataset</param>
        /// <param name="isMasterGettingInputData">Whether the master node should get a partition</param>
        public void AddDataset(IPartitionedInputDataSet inputDataSet, bool isMasterGettingInputData = false)
        {
            AddDataset(inputDataSet.Select(x => x.GetPartitionConfiguration()).ToArray(), isMasterGettingInputData);
        }

        /// <summary>
        /// Add a set of datasets to the stage.
        /// </summary>
        /// <param name="inputDataSet">The configuration for the datasets</param>
        /// <param name="isMasterGettingInputData">Whether the master node should get a partition</param>
        public void AddDataset(IConfiguration[] inputDataSet, bool isMasterGettingInputData = false)
        {
            _isMasterGettingInputData = isMasterGettingInputData;

            _datasetConfiguration = Optional<IConfiguration[]>.Of(inputDataSet);
        }

        /// <summary>
        /// Finalizes the stages.
        /// After the stages has been finalized, no more operators can
        /// be added to the group.
        /// </summary>
        /// <returns>The same finalized stages</returns>
        public IElasticStage Build()
        {
            if (_finalized == true)
            {
                throw new IllegalStateException("Stage cannot be built more than once");
            }

            if (_datasetConfiguration.IsPresent())
            {
                var adjust = _isMasterGettingInputData ? 0 : 1;

                if (_datasetConfiguration.Value.Length + adjust < _numTasks)
                {
                    throw new IllegalStateException(
                        "Dataset is smaller than the number of tasks: "
                        + $"re-submit with {_datasetConfiguration.Value.Length + adjust} tasks");
                }
            }

            PipelineRoot.GatherMasterIds(ref _masterTasks);

            _finalized = true;

            return this;
        }

        /// <summary>
        /// Add a task to the stages.
        /// The stages must have been built before tasks can be added.
        /// </summary>
        /// <param name="taskId">The id of the task to add</param>
        /// <returns>True if the task is correctly added to the stages</returns>
        public bool AddTask(string taskId)
        {
            if (string.IsNullOrEmpty(taskId))
            {
                throw new ArgumentException($"{nameof(taskId)} cannot be empty.");
            }

            if (IsCompleted || (_scheduled && FailureState.FailureState.IsFail()))
            {
                Log.Log(Level.Warning, "Taskset {0}." ,IsCompleted ? "completed." : "failed.");
                return false;
            }

            if (!_finalized)
            {
                throw new IllegalStateException("Stage must be finalized before adding tasks.");
            }

            lock (_tasksLock)
            {
                // We don't add a task if eventually we end up by not adding the master task
                var tooManyTasks = _tasksAdded >= _numTasks;
                var notAddingMaster = _tasksAdded + _missingMasterTasks.Count >= _numTasks &&
                    !_missingMasterTasks.Contains(taskId);

                if (!_scheduled && (tooManyTasks || notAddingMaster))
                {
                    if (tooManyTasks)
                    {
                        Log.Log(Level.Warning,
                            "Already added {0} tasks when total tasks request is {1}", _tasksAdded, _numTasks);
                    }

                    if (notAddingMaster)
                    {
                        Log.Log(Level.Warning,
                            "Already added {0} over {1} but missing master task(s)", _tasksAdded, _numTasks);
                    }

                    return false;
                }

                if (PipelineRoot.AddTask(taskId))
                {
                    _tasksAdded++;
                    _missingMasterTasks.Remove(taskId);
                    _failureMachine.AddDataPoints(1, false);
                }
            }

            return true;
        }

        /// <summary>
        /// Decides if the tasks added to the stages can be scheduled for execution
        /// or not. This method is used for implementing different policies for
        /// triggering the scheduling of tasks.
        /// </summary>
        /// <returns>True if the previously added tasks can be scheduled for execution</returns>
        public bool ScheduleStage()
        {
            // Schedule if we reach the number of requested tasks or the stage contains an iterative pipeline
            // that is ready to be scheduled and the policy requested by the user allow early start with ramp up.
            if (!_scheduled &&
                (_numTasks == _tasksAdded ||
                    (IsIterative &&
                        _failureMachine.State.FailureState < (int)DefaultFailureStates.StopAndReschedule &&
                        PipelineRoot.CanBeScheduled())))
            {
                _scheduled = true;

                PipelineRoot.BuildState();
            }

            return _scheduled;
        }

        /// <summary>
        /// Whether the input activeContext is the one of the master tasks.
        /// </summary>
        /// <param name="activeContext">The active context of the task</param>
        /// <returns>True if the input parameter is the master task's active context</returns>
        public bool IsMasterTaskContext(IActiveContext activeContext)
        {
            if (!_finalized)
            {
                throw new IllegalStateException("Driver must call Build() before checking IsMasterTaskContext.");
            }

            int id = Utils.GetContextNum(activeContext);
            return _masterTasks.Any(task => Utils.GetTaskNum(task) == id);
        }

        /// <summary>
        /// Creates the Configuration for the input task.
        /// Must be called only after all tasks have been added to the stages.
        /// </summary>
        /// <param name="builder">The configuration builder the configuration will be appended to</param>
        /// <param name="taskId">The task id of the task that belongs to this stages</param>
        /// <returns>The configuration for the Task with added stages informations</returns>
        public IConfiguration GetTaskConfiguration(int taskId)
        {
            ICsConfigurationBuilder confBuilder = TangFactory.GetTang().NewConfigurationBuilder();
            IList<string> serializedOperatorsConfs = new List<string>();

            PipelineRoot.GetTaskConfiguration(ref serializedOperatorsConfs, taskId);

            return confBuilder
                .BindStringNamedParam<Config.OperatorParameters.StageName>(StageName)
                .BindList<Config.OperatorParameters.SerializedOperatorConfigs, string>(serializedOperatorsConfs)
                .Build();
        }

        /// <summary>
        /// Given a task id, this method returns the configuration of the task's data partition
        /// (if any).
        /// </summary>
        /// <param name="taskId">The task id of the task we wanto to retrieve the data partition.
        /// The task is required to belong to thq stages</param>
        /// <returns>The configuration of the data partition (if any) of the task</returns>
        public Optional<IConfiguration> GetPartitionConf(string taskId)
        {
            if (!_datasetConfiguration.IsPresent() || (_masterTasks.Contains(taskId) && !_isMasterGettingInputData))
            {
                return Optional<IConfiguration>.Empty();
            }

            var index = Utils.GetTaskNum(taskId) - 1;
            index = _masterTasks.Count == 0 || _isMasterGettingInputData ? index : index - 1;

            if (index < 0 || index >= _datasetConfiguration.Value.Length)
            {
                throw new IllegalStateException($"Asking for a not existing partition configuration {index}.");
            }

            return Optional<IConfiguration>.Of(_datasetConfiguration.Value[index]);
        }

        /// <summary>
        /// Method used to signal that the stage state can be moved to complete.
        /// </summary>
        public void Complete()
        {
            lock (_statusLock)
            {
                FailureState = FailureState.Merge(_failureMachine.Complete());
            }
        }

        /// <summary>
        /// Retrieve the log the final statistics of the computation: this is the sum of all
        /// the stats of all the Operators compising the stage. This method can be called
        /// only once the stages is completed.
        /// </summary>
        /// <returns>The final statistics for the computation</returns>
        public string LogFinalStatistics()
        {
            if (IsCompleted || FailureState.FailureState.IsFail())
            {
                return PipelineRoot.LogFinalStatistics();
            }
            else
            {
                throw new IllegalStateException(
                    $"Cannot log statistics before Stage {StageName} is completed or failed.");
            }
        }

        /// <summary>
        /// Method triggered when a task to driver message is received.
        /// </summary>
        /// <param name="message">The task message for the operator</param>
        /// <param name="returnMessages">A list of messages containing the instructions for the task</param>
        /// <exception cref="IllegalStateException">If the message cannot be handled correctly or generate
        /// an incorrent state</exception>
        public void OnTaskMessage(ITaskMessage message, ref List<IElasticDriverMessage> returnMessages)
        {
            int offset = 0;
            var length = BitConverter.ToUInt16(message.Message, offset);
            offset += sizeof(ushort);
            var stageName = ByteUtilities.ByteArraysToString(message.Message, offset, length);
            offset += length;

            if (stageName == StageName)
            {
                // Messages have to be propagated down to the operators
                PipelineRoot.OnTaskMessage(message, ref returnMessages);
            }
        }

        #region Failure Response

        /// <summary>
        /// Used to react when a timeout event is triggered.
        /// </summary>
        /// <param name="alarm">The alarm triggering the timeput</param>
        /// <param name="msgs">A list of messages encoding how remote tasks need to react</param>
        /// <param name="nextTimeouts">The next timeouts to be scheduled</param>
        public void OnTimeout(Alarm alarm, ref List<IElasticDriverMessage> msgs, ref List<ITimeout> nextTimeouts)
        {
            PipelineRoot.OnTimeout(alarm, ref msgs, ref nextTimeouts);
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
            // Failures have to be propagated down to the operators
            PipelineRoot.OnTaskFailure(task, ref failureEvents);
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

                default:
                    OnFail();
                    break;
            }

            PipelineRoot.EventDispatcher(ref @event);
        }

        #endregion Failure Response

        #region Default Failure Events Response

        /// <summary>
        /// Mechanism to execute when a reconfigure event is triggered.
        /// <paramref name="reconfigureEvent"/>
        /// </summary>
        public void OnReconfigure(ref ReconfigureEvent reconfigureEvent)
        {
            lock (_statusLock)
            {
                FailureState = FailureState.Merge(
                    new DefaultFailureState((int)DefaultFailureStates.ContinueAndReconfigure));
            }
        }

        /// <summary>
        /// Mechanism to execute when a reschedule event is triggered.
        /// <paramref name="rescheduleEvent"/>
        /// </summary>
        public void OnReschedule(ref RescheduleEvent rescheduleEvent)
        {
            lock (_statusLock)
            {
                FailureState = FailureState.Merge(
                    new DefaultFailureState((int)DefaultFailureStates.ContinueAndReschedule));
            }
        }

        /// <summary>
        /// Mechanism to execute when a stop event is triggered.
        /// <paramref name="stopEvent"/>
        /// </summary>
        public void OnStop(ref StopEvent stopEvent)
        {
            lock (_statusLock)
            {
                FailureState = FailureState.Merge(
                    new DefaultFailureState((int)DefaultFailureStates.StopAndReschedule));
            }
        }

        /// <summary>
        /// Mechanism to execute when a fail event is triggered.
        /// </summary>
        public void OnFail()
        {
            lock (_statusLock)
            {
                FailureState = FailureState.Merge(new DefaultFailureState((int)DefaultFailureStates.Fail));
            }
        }

        #endregion Default Failure Events Response
    }
}