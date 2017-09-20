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
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Tang.Util;
using System.Collections.Generic;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Network.Elastic.Topology.Logical;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Network.Elastic.Config.OperatorsParameters;

namespace Org.Apache.REEF.Network.Elastic.Operators.Logical.Impl
{
    /// <summary>
    /// Basic implementation for logical operators.
    /// Each operator is part of a subscription and is parametrized by a topology, a failure
    /// state machine and a checkpoint policy.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public abstract class ElasticOperator : IFailureResponse
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(ElasticOperator));

        // For the moment we consider only linear sequences of operators (no branching for e.g., joins)
        protected ElasticOperator _next = null;
        protected ElasticOperator _prev = null;

        protected IFailureStateMachine _failureMachine;
        protected CheckpointLevel _checkpointLevel;
        protected ITopology _topology;

        protected bool _stateFinalized = false;
        protected bool _operatorFinalized = false;

        protected IElasticTaskSetSubscription _subscription;
        protected int _id;

        protected IConfiguration[] _configurations;

        /// <summary>
        /// Specification for generic Elastic Operators
        /// </summary>
        /// <param name="subscription">The subscription this operator is part of</param>
        /// <param name="prev">The previous operator in the pipeline</param>
        /// <param name="topology">The topology of the operator</param>
        /// <param name="failureMachine">The behaviour of the operator under failures</param>
        /// <param name="checkpointLevel">The checkpoint policy for the operator</param>
        /// <param name="configurations">Additional configuration parameters</param>
        public ElasticOperator(
            IElasticTaskSetSubscription subscription,
            ElasticOperator prev,
            ITopology topology,
            IFailureStateMachine failureMachine,
            CheckpointLevel checkpointLevel = CheckpointLevel.None,
            params IConfiguration[] configurations)
        {
            _subscription = subscription;
            _prev = prev;
            _id = Subscription.GetNextOperatorId();
            _topology = topology;
            _failureMachine = failureMachine;
            _checkpointLevel = checkpointLevel;
            _configurations = configurations;
        }

        /// <summary>
        /// The identifier of the master / coordinator node for this operator
        /// </summary>
        public int MasterId { get; protected set; }

        /// <summary>
        /// An operator type specific name
        /// </summary>
        public string OperatorName { get; protected set; }

        /// <summary>
        /// The Subscription this Operator is part of
        /// </summary>
        protected IElasticTaskSetSubscription Subscription
        {
            get
            {
                if (_subscription == null)
                {
                    if (_prev == null)
                    {
                        throw new IllegalStateException("The reference to the parent subscription is lost");
                    }

                    return _prev.Subscription;
                }

                return _subscription;
            }
        }

        /// <summary>
        /// Add a task to the Operator.
        /// The Operator must have called Build() before adding tasks.
        /// </summary>
        /// <param name="taskId">The id of the task to add</param>
        /// <returns>True if the task is added to the Operator</returns>
        public virtual bool AddTask(string taskId)
        {
            if (_operatorFinalized == false)
            {
                throw new IllegalStateException("Operator needs to be built before adding tasks");
            }

            _topology.AddTask(taskId);

            // The assumption for the moment is that only one data point is added for each task
            _failureMachine.AddDataPoints(1);

            if (_next != null)
            {
                _next.AddTask(taskId);
            }

            return true;
        }

        /// <summary>
        /// Appends the Operators configuration for the input task to the input builder.
        /// Must be called only after Build() and BuildState() have been called.
        /// This method shold be called from the root operator at beginning of the pipeline
        /// </summary>
        /// <param name="builder">The configuration builder the Operator configuration will be appended to</param>
        /// <param name="taskId">The task id of the task that belongs to this Operator</param>
        /// <returns>The configuration for the Task with added Operators information</returns>
        public void GetTaskConfiguration(ref ICsConfigurationBuilder builder, int taskId)
        {
            if (_operatorFinalized && _stateFinalized)
            {
                GetOperatorConfiguration(ref builder, taskId);

                if (_next != null)
                {
                    _next.GetTaskConfiguration(ref builder, taskId);
                }
            }
            else
            {
                throw new IllegalStateException("Operator needs to be built before getting tasks configuration");
            }
        }

        /// <summary>
        /// Finalizes the Operator.
        /// </summary>
        /// <returns>The same finalized Operator</returns>
        public virtual ElasticOperator Build()
        {
            if (_operatorFinalized == true)
            {
                throw new IllegalStateException("Operator cannot be built more than once");
            }

            if (_prev != null)
            {
                _prev.Build();
            }

            _operatorFinalized = true;

            return this;
        }

        /// <summary>
        /// Finalizes the Operator state. After BuildState, no more tasks can be added
        /// to the Operator
        /// </summary>
        /// <returns>The same Operator with the finalized state</returns>
        public virtual ElasticOperator BuildState()
        {
            if (_stateFinalized == true)
            {
                throw new IllegalStateException("Operator cannot be built more than once");
            }

            if (_operatorFinalized != true)
            {
                throw new IllegalStateException("Operator need to be build before finalizing its state");
            }

            if (_next != null)
            {
                _next.BuildState();
            }

            _failureMachine.Build();
            _topology.Build();

            LogOperatorState();

            _stateFinalized = true;

            return this;
        }

        /// <summary>
        /// Adds the Broadcast Operator to the operator pipeline.
        /// </summary>
        /// <typeparam name="T">The type of messages that operators will send / receive</typeparam>
        /// <param name="senderId">The id of the sender node</param>
        /// <param name="topology">The topology of the operator</param>
        /// <param name="failureMachine">The failure state machine of the operator</param>
        /// <param name="checkpointLevel">The checkpoint policy for the operator</param>
        /// <param name="configurations">The configuration of the tasks</param>
        /// <returns>The same operator pipeline with the added Broadcast operator</returns>
        public abstract ElasticOperator Broadcast<T>(int senderId, ITopology topology = null, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations);

        /// <summary>
        /// Adds an instance of the Broadcast Operator to the operator pipeline.
        /// </summary>
        /// <typeparam name="T">The type of messages that operators will send / receive</typeparam>
        /// <param name="senderId">The id of the sender node</param>
        /// <param name="configurations">The configuration of the tasks</param>
        /// <returns>The same operator pipeline with the added Broadcast operator</returns>
        public ElasticOperator Broadcast<T>(int senderId, params IConfiguration[] configurations)
        {
            return Broadcast<T>(senderId, null, _failureMachine.Clone(), CheckpointLevel.None, configurations);
        }

        /// <summary>
        /// Adds an instance of the Aggregation Ring Operator to the operator pipeline.
        /// </summary>
        /// <typeparam name="T">The type of messages that operators will send / receive</typeparam>
        /// <param name="coordinatorId">The id of the coordinator node starting the ring</param>
        /// <param name="failureMachine">The failure state machine of the operator</param>
        /// <param name="checkpointLevel">The checkpoint policy for the operator</param>
        /// <param name="configurations">The configuration of the tasks</param>
        /// <returns>The same operator pipeline with the added Aggregtion Ring operator</returns>
        public abstract ElasticOperator AggregationRing<T>(int coordinatorId, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations);

        /// <summary>
        /// Adds an instance of the Aggregation Ring Operator to the operator pipeline.
        /// </summary>
        /// <typeparam name="T">The type of messages that operators will send / receive</typeparam>
        /// <param name="configurations">The configuration of the tasks</param>
        /// <returns>The same operator pipeline with the added Aggregtion Ring operator</returns>
        public ElasticOperator AggregationRing<T>(params IConfiguration[] configurations)
        {
            return AggregationRing<T>(MasterId, _failureMachine.Clone(), CheckpointLevel.None, configurations);
        }

        /// <summary>
        /// Adds an instance of the Aggregation Ring Operator to the operator pipeline.
        /// </summary>
        /// <typeparam name="T">The type of messages that operators will send / receive</typeparam>
        /// <param name="checkpointLevel">The checkpoint policy for the operator</param>
        /// <param name="configurations">The configuration of the tasks</param>
        /// <returns>The same operator pipeline with the added Aggregtion Ring operator</returns>
        public ElasticOperator AggregationRing<T>(CheckpointLevel checkpointLevel, params IConfiguration[] configurations)
        {
            return AggregationRing<T>(MasterId, _failureMachine.Clone(), checkpointLevel, configurations);
        }

        /// <summary>
        /// Adds an instance of the Enumerable Iterate Operator to the operator pipeline.
        /// This Operator Iterate a user provided number of times.
        /// </summary>
        /// <param name="masterId">The id of the node coordinating the iterations</param>
        /// <param name="failureMachine">The failure state machine of the operator</param>
        /// <param name="checkpointLevel">The checkpoint policy for the operator</param>
        /// <param name="configurations">The configuration of the tasks</param>
        /// <returns>The same operator pipeline with the added Enumerable Iterate operator</returns>
        public abstract ElasticOperator EnumerableIterate(int masterId, IFailureStateMachine failureMachine = null, CheckpointLevel checkpointLevel = CheckpointLevel.None, params IConfiguration[] configurations);

        /// <summary>
        /// Adds an instance of the Enumerable Iterate Operator to the operator pipeline.
        /// This Operator Iterate a user provided number of times.
        /// </summary>
        /// <param name="failureMachine">The failure state machine of the operator</param>
        /// <param name="checkpointLevel">The checkpoint policy for the operator</param>
        /// <param name="configurations">The configuration of the tasks</param>
        /// <returns>The same operator pipeline with the added Enumerable Iterate operator</returns>
        public ElasticOperator Iterate(IFailureStateMachine failureMachine, CheckpointLevel checkpointLevel, params IConfiguration[] configurations)
        {
            return EnumerableIterate(MasterId, failureMachine, checkpointLevel, configurations);
        }

        /// <summary>
        /// Adds an instance of the Enumerable Iterate Operator to the operator pipeline.
        /// This Operator Iterate a user provided number of times.
        /// </summary>
        /// <param name="configurations">The configuration of the tasks</param>
        /// <returns>The same operator pipeline with the added Enumerable Iterate operator</returns>
        public ElasticOperator Iterate(params IConfiguration[] configurations)
        {
            return EnumerableIterate(MasterId, _failureMachine.Clone(), CheckpointLevel.None, configurations);
        }

        public abstract void OnTaskFailure(IFailedTask task, ref List<IFailureEvent> failureEvents);

        public abstract void EventDispatcher(IFailureEvent @event, ref List<IDriverMessage> failureResponses);

        /// <summary>
        /// Appends the Operator specific configuration for the input task to the input builder.
        /// This method is operator specific and serializes the operator configuration into the builder.
        /// </summary>
        /// <param name="builder">The configuration builder the Operator configuration will be appended to</param>
        /// <param name="taskId">The task id of the task that belongs to this Operator</param>
        /// <returns>The configuration for the Task with added serialized Operator conf</returns>
        protected virtual void GetOperatorConfiguration(ref ICsConfigurationBuilder builder, int taskId)
        {
            ICsConfigurationBuilder operatorBuilder = TangFactory.GetTang().NewConfigurationBuilder();

            _topology.GetTaskConfiguration(ref operatorBuilder, taskId);

            PhysicalOperatorConfiguration(ref operatorBuilder);

            IConfiguration operatorConf = operatorBuilder
                .BindNamedParameter<OperatorId, int>(
                    GenericType<OperatorId>.Class,
                    _id.ToString(CultureInfo.InvariantCulture))
                .Build();

            foreach (var conf in _configurations)
            {
                operatorConf = Configurations.Merge(operatorConf, conf);
            }

            Subscription.Service.SerializeOperatorConfiguration(ref builder, operatorConf);
        }

        /// <summary>
        /// Binding from logical to physical operator. 
        /// </summary>
        /// <param name="builder">The configuration builder the binding will be added to</param>
        /// <returns>The configuration for the Task with added logical-to-physical binding</returns>
        protected abstract void PhysicalOperatorConfiguration(ref ICsConfigurationBuilder builder);

        /// <summary>
        /// Appends the message type to the configuration. 
        /// </summary>
        /// <param name="operatorType">The type of the messages the operator is configured to accept</param>
        /// <param name="builder">The configuration builder the message type will be added to</param>
        /// <returns>The configuration for the Task with added message type information</returns>
        protected static void SetMessageType(Type operatorType, ref ICsConfigurationBuilder confBuilder)
        {
            if (operatorType.IsGenericType)
            {
                var genericTypes = operatorType.GenericTypeArguments;
                var msgType = genericTypes[0];
                confBuilder.BindNamedParameter<MessageType, string>(
                    GenericType<MessageType>.Class, msgType.AssemblyQualifiedName);
            }
        }

        /// <summary>
        /// Logs the current operator state 
        /// </summary>
        protected virtual void LogOperatorState()
        {
            string intro = string.Format(CultureInfo.InvariantCulture,
               "State for Operator {0} in Subscription {1}:\n", OperatorName, Subscription.SubscriptionName);
            string topologyState = string.Format(CultureInfo.InvariantCulture, "Topology:\n{0}", _topology.LogTopologyState());
            string failureMachineState = "Failure State: " + _failureMachine.State.FailureState +
                    "\nFailure(s) Reported: " + _failureMachine.NumOfFailedDataPoints;

            LOGGER.Log(Level.Info, intro + topologyState + failureMachineState);
        }

        /// <summary>
        /// Returns whether a failure should be propagated to downstream operators or not  
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
        protected virtual bool ReactOnTaskMessage(ITaskMessage message, ref List<IDriverMessage> returnMessages)
        {
            return false;
        }

        /// <summary>
        /// Utility method gathering the set of master task ids of the operators in the current pipeline.
        /// </summary>
        /// <param name="masterTasks">The id of the master tasks of the operators preceding operators</param>
        internal virtual void GatherMasterIds(ref HashSet<string> masterTasks)
        {
            if (_operatorFinalized != true)
            {
                throw new IllegalStateException("Operator need to be build before finalizing the subscription");
            }

            masterTasks.Add(Utils.BuildTaskId(Subscription.SubscriptionName, MasterId));

            if (_next != null)
            {
                _next.GatherMasterIds(ref masterTasks);
            }
        }
    }
}