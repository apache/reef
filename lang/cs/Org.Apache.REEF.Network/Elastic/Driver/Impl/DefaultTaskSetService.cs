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
using System.Net;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Failures.Impl;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Wake.Time.Event;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Driver.Impl
{
    /// <summary>
    /// Default implementation for the task service.
    /// This is mainly used to create subscription.
    /// Also manages configurations for Elastic Group Communication operators/services.
    /// </summary>
    [Unstable("0.16", "API may change")]
    public sealed class DefaultTaskSetService : IElasticTaskSetService, IDefaultFailureEventResponse
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultTaskSetService));

        private readonly string _driverId;
        private readonly int _numEvaluators;
        private readonly string _nameServerAddr;
        private readonly int _nameServerPort;
        private readonly INameServer _nameServer;
        private readonly string _defaultSubscriptionName;
        private readonly IFailureStateMachine _defaultFailureMachine;

        private readonly Dictionary<string, IElasticTaskSetSubscription> _subscriptions;
        private readonly AvroConfigurationSerializer _configSerializer;

        private readonly object _subsLock = new object();
        private readonly object _statusLock = new object();

        private IFailureState _failureStatus;

        [Inject]
        private DefaultTaskSetService(
            [Parameter(typeof(ElasticServiceConfigurationOptions.DriverId))] string driverId,
            [Parameter(typeof(ElasticServiceConfigurationOptions.DefaultSubscriptionName))] string defaultSubscriptionName,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NumEvaluators))] int numEvaluators,
            AvroConfigurationSerializer configSerializer,
            INameServer nameServer,
            IFailureStateMachine defaultFailureStateMachine)
        {
            _driverId = driverId;
            _numEvaluators = numEvaluators;
            _defaultSubscriptionName = defaultSubscriptionName;
            _defaultFailureMachine = defaultFailureStateMachine;

            _failureStatus = new DefaultFailureState();
            _configSerializer = configSerializer;
            _subscriptions = new Dictionary<string, IElasticTaskSetSubscription>();

            _nameServer = nameServer;
            IPEndPoint localEndpoint = nameServer.LocalEndpoint;
            _nameServerAddr = localEndpoint.Address.ToString();
            _nameServerPort = localEndpoint.Port;
        }

        /// <summary>
        /// Returns a subscription with the default settings (default name and failure machine).
        /// </summary>
        /// <returns>A subscription with default settings</returns>
        public IElasticTaskSetSubscription DefaultTaskSetSubscription()
        {
            lock (_subsLock)
            {
                IElasticTaskSetSubscription defaultSubscription;
                _subscriptions.TryGetValue(_defaultSubscriptionName, out defaultSubscription);

                if (defaultSubscription == null)
                {
                    NewTaskSetSubscription(_defaultSubscriptionName, _numEvaluators, _defaultFailureMachine.Clone(_numEvaluators, (int)DefaultFailureStates.Fail));
                }

                return _subscriptions[_defaultSubscriptionName];
            }
        }

        /// <summary>
        /// Creates a new subscription.
        ///  The subscription lifecicle is managed by the service.
        /// </summary>
        /// <param name="subscriptionName">The name of the subscription</param>
        /// <param name="numTasks">The number of tasks required by the subscription</param>
        /// <param name="failureMachine">An optional failure machine governing the subscription</param>
        /// <returns>The new task Set subscrption</returns>
        public IElasticTaskSetSubscription NewTaskSetSubscription(
            string subscriptionName,
            int numTasks,
            IFailureStateMachine failureMachine = null)
        {
            if (string.IsNullOrEmpty(subscriptionName))
            {
                throw new ArgumentNullException($"{nameof(subscriptionName)} cannot be null.");
            }

            if (numTasks <= 0)
            {
                throw new ArgumentException($"{nameof(numTasks)} is required to be greater than 0.");
            }

            lock (_subsLock)
            {
                if (_subscriptions.ContainsKey(subscriptionName))
                {
                    throw new ArgumentException($"Subscription {subscriptionName} already registered with the service.");
                }

                var subscription = new DefaultTaskSetSubscription(
                    subscriptionName,
                    numTasks,
                    this,
                    failureMachine ?? _defaultFailureMachine.Clone(numTasks, (int)DefaultFailureStates.Fail));
                _subscriptions[subscriptionName] = subscription;

                return subscription;
            }
        }

        /// <summary>
        /// Remove a task Set subscription from the service.
        /// </summary>
        /// <param name="subscriptionName">The name of the subscription to be removed</param>
        public void RemoveTaskSetSubscription(string subscriptionName)
        {
            lock (_subsLock)
            {
                if (!_subscriptions.ContainsKey(subscriptionName))
                {
                    throw new ArgumentException($"Subscription {subscriptionName} is not registered with the service.");
                }

                _subscriptions.Remove(subscriptionName);
            }
        }

        /// <summary>
        /// Get the subscriptions names from the context.
        /// </summary>
        /// <param name="activeContext">An activeContext</param>
        /// <returns>The subscriptions representented in the context</returns>
        public string GetContextSubscriptions(IActiveContext activeContext)
        {
            return Utils.GetContextSubscriptions(activeContext);
        }

        /// <summary>
        /// Generate the service configuration object.
        /// This method is used to properly configure Contexts with the service.
        /// </summary>
        /// <returns>The service Configuration</returns>
        public IConfiguration GetServiceConfiguration()
        {
            IConfiguration serviceConfig = ServiceConfiguration.ConfigurationModule
                .Set(ServiceConfiguration.Services,
                    GenericType<StreamingNetworkService<GroupCommunicationMessage>>.Class)
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(serviceConfig)
                .BindNamedParameter<NamingConfigurationOptions.NameServerAddress, string>(
                    GenericType<NamingConfigurationOptions.NameServerAddress>.Class,
                    _nameServerAddr)
                .BindNamedParameter<NamingConfigurationOptions.NameServerPort, int>(
                    GenericType<NamingConfigurationOptions.NameServerPort>.Class,
                    _nameServerPort.ToString(CultureInfo.InvariantCulture))
                .BindImplementation(GenericType<INameClient>.Class,
                    GenericType<NameClient>.Class)

                .Build();
        }

        /// <summary>
        /// Creates a generic task Configuration object for the tasks registering to the service.
        /// </summary>
        /// <param name="subscriptionsConf">The configuration of the subscription the task will register to</param>
        /// <returns>The configuration for the task with added service parameters</returns>
        public IConfiguration GetTaskConfiguration(ICsConfigurationBuilder subscriptionsConf)
        {
            return subscriptionsConf
                .BindNamedParameter<ElasticServiceConfigurationOptions.DriverId, string>(
                    GenericType<ElasticServiceConfigurationOptions.DriverId>.Class,
                    _driverId)
                .Build();
        }

        /// <summary>
        /// Appends a subscription configuration to a configuration builder object.
        /// </summary>
        /// <param name="confBuilder">The configuration where the subscription configuration will be appended to</param>
        /// <param name="subscriptionConf">The subscription configuration at hand</param>
        /// <returns>The configuration containing the serialized subscription configuration</returns>
        public void SerializeSubscriptionConfiguration(ref ICsConfigurationBuilder confBuilder, IConfiguration subscriptionConfiguration)
        {
            confBuilder.BindSetEntry<ElasticServiceConfigurationOptions.SerializedSubscriptionConfigs, string>(
                GenericType<ElasticServiceConfigurationOptions.SerializedSubscriptionConfigs>.Class,
                _configSerializer.ToString(subscriptionConfiguration));
        }

        /// <summary>
        /// Append an operator configuration to a configuration builder object.
        /// </summary>
        /// <param name="serializedOperatorsConfs">The list where the operator configuration will be appended to</param>
        /// <param name="subscriptionConf">The operator configuration at hand</param>
        /// <returns>The configuration containing the serialized operator configuration</returns>
        public void SerializeOperatorConfiguration(ref IList<string> serializedOperatorsConfs, IConfiguration operatorConfiguration)
        {
            serializedOperatorsConfs.Add(_configSerializer.ToString(operatorConfiguration));
        }

        #region Failure Response
        /// <summary>
        /// Used to react on a failure occurred on a task.
        /// It gets a failed task as input and in response it produces zero or more failure events.
        /// </summary>
        /// <param name="task">The failed task</param>
        /// <param name="failureEvents">A list of events encoding the type of actions to be triggered so far</param>
        public void OnTaskFailure(IFailedTask value, ref List<IFailureEvent> failureEvents)
        {
            var task = value.Id;
            _nameServer.Unregister(task);
        }

        /// <summary>
        /// Used to react when a timeout event is triggered.
        /// It gets a failed task as input and in response it produces zero or more failure events.
        /// </summary>
        /// <param name="alarm">The alarm triggering the timeput</param>
        /// <param name="msgs">A list of messages encoding how remote Tasks need to reach</param>
        /// /// <param name="nextTimeouts">The next timeouts to be scheduled</param>
        public void OnTimeout(Alarm alarm, ref List<IElasticDriverMessage> msgs, ref List<ITimeout> nextTimeouts)
        {
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
                    var rec = @event as IReconfigure;
                    OnReconfigure(ref rec);
                    break;
                case DefaultFailureStateEvents.Reschedule:
                    var res = @event as IReschedule;
                    OnReschedule(ref res);
                    break;
                case DefaultFailureStateEvents.Stop:
                    var stp = @event as IStop;
                    OnStop(ref stp);
                    break;
                default:
                    OnFail();
                    break;
            }
        }

        #endregion

        #region Default Failure event Response
        /// <summary>
        /// Mechanism to execute when a reconfigure event is triggered.
        /// <paramref name="reconfigureEvent"/>
        /// </summary>
        public void OnReconfigure(ref IReconfigure info)
        {
            lock (_statusLock)
            {
                _failureStatus = _failureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.ContinueAndReconfigure));
            }
        }

        /// <summary>
        /// Mechanism to execute when a reschedule event is triggered.
        /// <paramref name="rescheduleEvent"/>
        /// </summary>
        public void OnReschedule(ref IReschedule rescheduleEvent)
        {
            lock (_statusLock)
            {
                _failureStatus = _failureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.ContinueAndReschedule));
            }
        }

        /// <summary>
        /// Mechanism to execute when a stop event is triggered.
        /// <paramref name="stopEvent"/>
        /// </summary>
        public void OnStop(ref IStop stopEvent)
        {
            lock (_statusLock)
            {
                _failureStatus = _failureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.StopAndReschedule));
            }
        }

        /// <summary>
        /// Mechanism to execute when a fail event is triggered.
        /// </summary>
        public void OnFail()
        {
            lock (_statusLock)
            {
                _failureStatus = _failureStatus.Merge(new DefaultFailureState((int)DefaultFailureStates.Fail));
            }
        }
        #endregion
    }
}
