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
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Network.Elastic.Driver;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Utilities.Logging;
using System.Globalization;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using System.Collections.Generic;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Network.Elastic.Topology.Logical;
using Org.Apache.REEF.Network.Elastic.Topology.Logical.Impl;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Wake.Time.Event;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Network.Elastic.Topology.Logical.Enum;
using Org.Apache.REEF.Wake.StreamingCodec.CommonStreamingCodecs;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical
{
    /// <summary>
    /// Basic implementation for logical operators.
    /// Each operator is part of a stage and is parametrized by a topology, a failure
    /// state machine and a checkpoint policy.
    /// Operators are composed into pipelines.
    /// Once a pipeline is finalized, tasks can be added to the operator, which
    /// will in turn add the tasks to the topology and the failure state machine.
    /// When no more tasks are added, the operator state must be finalized in order to
    /// schedule the pipeline for execution.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public abstract class ElasticOperator : IFailureResponse, ITaskMessageResponse
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ElasticOperator));

        protected static readonly Dictionary<Type, IConfiguration> CODECMAP = new Dictionary<Type, IConfiguration>()
        {
            {
                typeof(int), StreamingCodecConfiguration<int>.Conf
                .Set(StreamingCodecConfiguration<int>.Codec, GenericType<IntStreamingCodec>.Class)
                .Build()
            },
            {
                typeof(int[]), StreamingCodecConfiguration<int[]>.Conf
                .Set(StreamingCodecConfiguration<int[]>.Codec, GenericType<IntArrayStreamingCodec>.Class)
                .Build()
            },
            {
                typeof(float), StreamingCodecConfiguration<float>.Conf
                .Set(StreamingCodecConfiguration<float>.Codec, GenericType<FloatStreamingCodec>.Class)
                .Build()
            },
            {
                typeof(float[]), StreamingCodecConfiguration<float[]>.Conf
                .Set(StreamingCodecConfiguration<float[]>.Codec, GenericType<FloatArrayStreamingCodec>.Class)
                .Build()
            }

        };

        // For the moment we consider only linear sequences (pipelines) of operators (no branching for e.g., joins)
        protected ElasticOperator _next = null;
        protected readonly ElasticOperator _prev;

        protected readonly IFailureStateMachine _failureMachine;
        protected readonly CheckpointLevel _checkpointLevel;
        protected readonly ITopology _topology;
        protected readonly int _id;
        protected readonly IConfiguration[] _configurations;

        protected bool _operatorFinalized;
        protected volatile bool _operatorStateFinalized;
        protected IElasticStage _stage;

        /// <summary>
        /// Specification for generic elastic operators.
        /// </summary>
        /// <param name="stage">The stage this operator is part of</param>
        /// <param name="prev">The previous operator in the pipeline</param>
        /// <param name="topology">The topology of the operator</param>
        /// <param name="failureMachine">The behavior of the operator under failures</param>
        /// <param name="checkpointLevel">The checkpoint policy for the operator</param>
        /// <param name="configurations">Additional configuration parameters</param>
        public ElasticOperator(
            IElasticStage stage,
            ElasticOperator prev,
            ITopology topology,
            IFailureStateMachine failureMachine,
            CheckpointLevel checkpointLevel = CheckpointLevel.None,
            params IConfiguration[] configurations)
        {
            _stage = stage;
            _prev = prev;
            _id = Stage.GetNextOperatorId();
            _topology = topology;
            _failureMachine = failureMachine;
            _checkpointLevel = checkpointLevel;
            _configurations = configurations;
            _operatorFinalized = false;
            _operatorStateFinalized = false;

            _topology.OperatorId = _id;
            _topology.StageName = Stage.StageName;
        }

        /// <summary>
        /// The identifier of the master / coordinator node for this operator.
        /// </summary>
        public int MasterId { get; protected set; }

        /// <summary>
        /// An operator type specific name.
        /// </summary>
        public string OperatorName { get; protected set; }

        /// <summary>
        /// Whether the current operator is or is preeceded by an iterator operator.
        /// </summary>
        public bool WithinIteration { get; protected set; }

        /// <summary>
        /// The stage this operator is part of.
        /// </summary>
        public IElasticStage Stage
        {
            get
            {
                if (_stage == null)
                {
                    if (_prev == null)
                    {
                        throw new IllegalStateException("The reference to the parent stage is lost.");
                    }

                    _stage = _prev.Stage;

                    return _prev.Stage;
                }

                return _stage;
            }
        }

        /// <summary>
        /// Add an instance of the broadcast operator to the operator pipeline
        /// with default failure machine and no checkpointing.
        /// </summary>
        /// <typeparam name="T">The type of messages that the operator will send / receive</typeparam>
        /// <param name="topology">The topology of the operator</param>
        /// <param name="configurations">Additional configurations for the operator</param>
        /// <returns>The same operator pipeline with the added broadcast operator</returns>
        public ElasticOperator Broadcast<T>(TopologyType topology, params IConfiguration[] configurations)
        {
            return Broadcast<T>(MasterId, GetTopology(topology), _failureMachine.Clone(), CheckpointLevel.None, configurations);
        }

        /// <summary>
        /// Add the broadcast operator to the operator pipeline
        /// with default failure machine.
        /// </summary>
        /// <typeparam name="T">The type of messages that the operator will send / receive</typeparam>
        /// <param name="topology">The topology of the operator</param>
        /// <param name="checkpointLevel">The checkpoint policy for the operator</param>
        /// <param name="configurations">Additional configurations for the operator</param>
        /// <returns>The same operator pipeline with the added broadcast operator</returns>
        public ElasticOperator Broadcast<T>(TopologyType topology, CheckpointLevel checkpointLevel, params IConfiguration[] configurations)
        {
            return Broadcast<T>(MasterId, GetTopology(topology), _failureMachine.Clone(), checkpointLevel, configurations);
        }

        /// <summary>
        /// Method triggered when a task to driver message is received. 
        /// This method eventually propagate tasks message through the pipeline.
        /// </summary>
        /// <param name="message">The task message for the operator</param>
        /// <param name="returnMessages">A list of messages containing the instructions for the task</param>
        /// <returns>True if the message was managed correctly, false otherwise</returns>
        public void OnTaskMessage(ITaskMessage message, ref List<IElasticDriverMessage> returnMessages)
        {
            var hasReacted = ReactOnTaskMessage(message, ref returnMessages);

            if (!hasReacted && _next != null)
            {
                _next.OnTaskMessage(message, ref returnMessages);
            }
        }

        /// <summary>
        /// Add a task to the operator.
        /// The operator must have called Build() before adding tasks.
        /// </summary>
        /// <param name="taskId">The id of the task to add</param>
        /// <returns>True if the task is new and is added to the operator</returns>
        public virtual bool AddTask(string taskId)
        {
            if (!_operatorFinalized)
            {
                throw new IllegalStateException("Operator needs to be finalized before adding tasks.");
            }

            //if (_operatorStateFinalized)
            //{
            //    throw new IllegalStateException("Task cannot be added to an operator with finalized state.");
            //}

            var newTask = _topology.AddTask(taskId, _failureMachine);

            if (_next != null)
            {
                // A task is new if it got added by at least one operator
                return _next.AddTask(taskId) || newTask;
            }

            return newTask;
        }

        /// <summary>
        /// Finalizes the operator.
        /// </summary>
        /// <returns>The same finalized operator</returns>
        public virtual ElasticOperator Build()
        {
            if (_operatorFinalized)
            {
                throw new IllegalStateException("Operator cannot be built more than once.");
            }

            if (_prev != null)
            {
                _prev.Build();
            }

            _operatorFinalized = true;

            return this;
        }

        /// <summary>
        /// Finalizes the operator state. After BuildState, no more tasks can be added
        /// to the Operator.
        /// </summary>
        /// <returns>The same operator with the finalized state</returns>
        public virtual ElasticOperator BuildState()
        {
            if (_operatorStateFinalized)
            {
                throw new IllegalStateException("Operator state cannot be built more than once.");
            }

            if (!_operatorFinalized)
            {
                throw new IllegalStateException("Operator need to be build before finalizing its state.");
            }

            if (_next != null)
            {
                _next.BuildState();
            }

            _topology.Build();

            LogOperatorState();

            _operatorStateFinalized = true;

            return this;
        }

        /// <summary>
        /// Generate the data serializer configuration for the target operator.
        /// </summary>
        /// <param name="confBuilder">The conf builder where to attach the codec configuration</param>
        internal virtual void GetCodecConfiguration(ref IConfiguration confBuilder)
        {
            if (_next != null)
            {
                _next.GetCodecConfiguration(ref confBuilder);
            }
        }

        /// <summary>
        /// Whether this is the last iterator in the pipeline.
        /// </summary>
        /// <returns>True if this is the last iterator</returns>
        public virtual bool CheckIfLastIterator()
        {
            if (_next == null)
            {
                return true;
            }

            return _next.CheckIfLastIterator();
        }

        /// <summary>
        /// Add the broadcast operator to the operator pipeline.
        /// </summary>
        /// <typeparam name="T">The type of messages that the operator will send / receive</typeparam>
        /// <param name="senderId">The id of the sender / root node of the broadcast</param>
        /// <param name="topology">The topology of the operator</param>
        /// <param name="failureMachine">The failure state machine of the operator</param>
        /// <param name="checkpointLevel">The checkpoint policy for the operator</param>
        /// <param name="configurations">Additional configurations for the operator</param>
        /// <returns>The same operator pipeline with the added broadcast operator</returns>
        public abstract ElasticOperator Broadcast<T>(int senderId, ITopology topology, IFailureStateMachine failureMachine, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations);

        /// <summary>
        /// Used to react on a failure occurred on a task.
        /// It gets a failed task as input and in response it produces zero or more failure events.
        /// </summary>
        /// <param name="task">The failed task</param>
        /// <param name="failureEvents">A list of events encoding the type of actions to be triggered so far</param>
        public abstract void OnTaskFailure(IFailedTask task, ref List<IFailureEvent> failureEvents);

        /// <summary>
        /// Used to react when a timeout event is triggered.
        /// It gets a failed task as input and in response it produces zero or more failure events.
        /// </summary>
        /// <param name="alarm">The alarm triggering the timeput</param>
        /// <param name="msgs">A list of messages encoding how remote Tasks need to reach</param>
        /// <param name="nextTimeouts">The next timeouts to be scheduled</param>
        public abstract void OnTimeout(Alarm alarm, ref List<IElasticDriverMessage> msgs, ref List<ITimeout> nextTimeouts);

        /// <summary>
        /// When a new failure state is reached, this method is used to dispatch
        /// such event to the proper failure mitigation logic.
        /// It gets a failure event as input and produces zero or more failure response messages
        /// for tasks (appended into the event).
        /// </summary>
        /// <param name="event">The failure event to react upon</param>
        public abstract void EventDispatcher(ref IFailureEvent @event);

        /// <summary>
        /// Appends the operator configuration for the input task to the input configuration.
        /// Must be called only after Build() and BuildState() have been called.
        /// This method should be called from the root operator at beginning of the pipeline.
        /// </summary>
        /// <param name="serializedOperatorsConfs">The list the operator configuration we will be appending to</param>
        /// <param name="taskId">The id of the task that belongs to this operator</param>
        /// <returns>The configuration for the task with added operator information</returns>
        internal void GetTaskConfiguration(ref IList<string> serializedOperatorsConfs, int taskId)
        {
            if (_operatorFinalized && _operatorStateFinalized)
            {
                GetOperatorConfiguration(ref serializedOperatorsConfs, taskId);

                if (_next != null)
                {
                    _next.GetTaskConfiguration(ref serializedOperatorsConfs, taskId);
                }
            }
            else
            {
                throw new IllegalStateException("Operator needs to be finalized before getting tasks configuration.");
            }
        }

        /// <summary>
        /// Whether this operator is ready to be scheduled by the task set manager.
        /// </summary>
        /// <returns>True if the operator is ready to be scheduled</returns>
        internal bool CanBeScheduled()
        {
            bool canBeScheduled = _topology.CanBeScheduled();

            if (canBeScheduled && _next != null)
            {
                return _next.CanBeScheduled();
            }

            return canBeScheduled;
        }

        /// <summary>
        /// Utility method gathering the set of master task ids of the operators in the current pipeline.
        /// </summary>
        /// <param name="masterTasks">The id of the master tasks of the current and successive operators</param>
        internal virtual void GatherMasterIds(ref HashSet<string> masterTasks)
        {
            if (_operatorFinalized != true)
            {
                throw new IllegalStateException("Operator need to be build before gathering information.");
            }

            masterTasks.Add(Utils.BuildTaskId(Stage.StageName, MasterId));

            if (_next != null)
            {
                _next.GatherMasterIds(ref masterTasks);
            }
        }

        /// <summary>
        /// Log the final statistics of the operator.
        /// This is called when the pipeline execution is completed.
        /// </summary>
        internal virtual string LogFinalStatistics()
        {
            var str = LogInternalStatistics();

            if (_next != null)
            {
                str += _next.LogFinalStatistics();
            }

            return str;
        }

        /// <summary>
        /// Appends the message type to the configuration. 
        /// </summary>
        /// <param name="operatorType">The type of the messages the operator is configured to accept</param>
        /// <param name="confBuilder">The configuration builder the message type will be added to</param>
        protected void SetMessageType(Type operatorType, ref ICsConfigurationBuilder confBuilder)
        {
            if (operatorType.IsGenericType)
            {
                var genericTypes = operatorType.GenericTypeArguments;
                var msgType = genericTypes[0];
                confBuilder.BindNamedParameter<OperatorParameters.MessageType, string>(
                    GenericType<OperatorParameters.MessageType>.Class, msgType.AssemblyQualifiedName);
            }
            else
            {
                throw new IllegalStateException("Expecting a generic type for the message.");
            }
        }

        /// <summary>
        /// Action to trigger when the operator recdeives a notification that a new iteration is started.
        /// </summary>
        /// <param name="iteration">The new iteration number</param>
        protected void OnNewIteration(int iteration)
        {
            _topology.OnNewIteration(iteration);

            if (_next != null)
            {
                _next.OnNewIteration(iteration);
            }
        }

        /// <summary>
        /// This method is operator specific and serializes the operator configuration into the input list.
        /// </summary>
        /// <param name="serializedOperatorsConfs">A list the serialized operator configuration will be appended to</param>
        /// <param name="taskId">The task id of the task that belongs to this operator</param>
        protected virtual void GetOperatorConfiguration(ref IList<string> serializedOperatorsConfs, int taskId)
        {
            ICsConfigurationBuilder operatorBuilder = TangFactory.GetTang().NewConfigurationBuilder();

            _topology.GetTaskConfiguration(ref operatorBuilder, taskId);

            PhysicalOperatorConfiguration(ref operatorBuilder);

            if (!Stage.IsIterative && _next == null)
            {
                operatorBuilder.BindNamedParameter<OperatorParameters.IsLast, bool>(
                    GenericType<OperatorParameters.IsLast>.Class,
                    true.ToString(CultureInfo.InvariantCulture));
            }

            IConfiguration operatorConf = operatorBuilder
                .BindNamedParameter<OperatorParameters.OperatorId, int>(
                    GenericType<OperatorParameters.OperatorId>.Class,
                    _id.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<OperatorParameters.Checkpointing, int>(
                    GenericType<OperatorParameters.Checkpointing>.Class,
                    ((int)_checkpointLevel).ToString(CultureInfo.InvariantCulture))
                .Build();

            foreach (var conf in _configurations)
            {
                operatorConf = Configurations.Merge(operatorConf, conf);
            }

            Stage.Context.SerializeOperatorConfiguration(ref serializedOperatorsConfs, operatorConf);
        }

        /// <summary>
        /// Returns whether a failure should be propagated to downstream operators or not.
        /// </summary>
        /// <returns>True if the failure has to be sent downstream</returns>
        protected virtual bool PropagateFailureDownstream()
        {
            return true;
        }

        /// <summary>
        /// Operator specific logic for reacting when a task message is received.
        /// </summary>
        /// <param name="message">Incoming message from a task</param>
        /// <param name="returnMessages">Zero or more reply messages for the task</param>
        /// <returns>True if the operator has reacted to the task message</returns>
        protected virtual bool ReactOnTaskMessage(ITaskMessage message, ref List<IElasticDriverMessage> returnMessages)
        {
            return false;
        }

        /// <summary>
        /// Logs the current operator state. 
        /// </summary>
        protected virtual void LogOperatorState()
        {
            string intro = $"State for Operator {OperatorName} in Stage {Stage.StageName}:\n";
            string topologyState = $"Topology:\n{_topology.LogTopologyState()}";
            string failureMachineState = "Failure State: " + _failureMachine.State.FailureState +
                    "\nFailure(s) Reported: " + _failureMachine.NumOfFailedDataPoints;

            LOGGER.Log(Level.Info, intro + topologyState + failureMachineState);
        }

        /// <summary>
        /// Log the final internal statistics of the operator.
        /// </summary>
        protected virtual string LogInternalStatistics()
        {
            return _topology.LogFinalStatistics();
        }

        /// <summary>
        /// Binding from logical to physical operator. 
        /// </summary>
        /// <param name="builder">The configuration builder the binding will be added to</param>
        protected abstract void PhysicalOperatorConfiguration(ref ICsConfigurationBuilder builder);

        private ITopology GetTopology(TopologyType topologyType)
        {
            ITopology topology;

            switch (topologyType)
            {
                case TopologyType.Flat:
                    topology = new FlatTopology(MasterId);
                    break;
                default: throw new ArgumentException(nameof(topologyType), $"Topology type {topologyType} not supported by {OperatorName}.");
            }

            return topology;
        }
    }
}
