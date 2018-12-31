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
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Comm;
using Org.Apache.REEF.Wake.Time.Event;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Wake.Remote.Parameters;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Elastic.Task.Impl;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Network.Elastic.Failures.Default;

namespace Org.Apache.REEF.Network.Elastic.Driver.Default
{
    /// <summary>
    /// Default implementation for the task context.
    /// This is mainly used to create stage.
    /// Also manages configurations for Elastic Group Communication operators/contexts.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal sealed class DefaultElasticContext : IElasticContext, IDefaultFailureEventResponse
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultElasticContext));

        private readonly int _startingPort;
        private readonly int _portRange;
        private readonly string _driverId;
        private readonly int _numEvaluators;
        private readonly string _nameServerAddr;
        private readonly int _nameServerPort;
        private readonly INameServer _nameServer;
        private readonly string _defaultStageName;
        private readonly IFailureStateMachine _defaultFailureMachine;
        private readonly IEvaluatorRequestor _evaluatorRequestor;
        private readonly int _memory;
        private readonly int _cores;
        private readonly string _batchId;
        private readonly string _rackName;

        private readonly Dictionary<string, IElasticStage> _stages;
        private readonly AvroConfigurationSerializer _configSerializer;

        private readonly object _subsLock = new object();
        private readonly object _statusLock = new object();

        private IFailureState _failureStatus;

        [Inject]
        private DefaultElasticContext(
            [Parameter(typeof(ElasticServiceConfigurationOptions.StartingPort))] int startingPort,
            [Parameter(typeof(ElasticServiceConfigurationOptions.PortRange))] int portRange,
            [Parameter(typeof(ElasticServiceConfigurationOptions.DriverId))] string driverId,
            [Parameter(typeof(ElasticServiceConfigurationOptions.DefaultStageName))] string defaultStageName,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NumEvaluators))] int numEvaluators,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NewEvaluatorMemorySize))] int memory,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NewEvaluatorNumCores))] int cores,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NewEvaluatorBatchId))] string batchId,
            [Parameter(typeof(ElasticServiceConfigurationOptions.NewEvaluatorRackName))] string rackName,
            AvroConfigurationSerializer configSerializer,
            IEvaluatorRequestor evaluatorRequestor,
            INameServer nameServer,
            IFailureStateMachine defaultFailureStateMachine)
        {
            _startingPort = startingPort;
            _portRange = portRange;
            _driverId = driverId;
            _numEvaluators = numEvaluators;
            _defaultStageName = defaultStageName;
            _defaultFailureMachine = defaultFailureStateMachine;
            _evaluatorRequestor = evaluatorRequestor;
            _memory = memory;
            _cores = cores;
            _batchId = batchId;
            _rackName = rackName;

            _failureStatus = new DefaultFailureState();
            _configSerializer = configSerializer;
            _stages = new Dictionary<string, IElasticStage>();

            _nameServer = nameServer;
            IPEndPoint localEndpoint = nameServer.LocalEndpoint;
            _nameServerAddr = localEndpoint.Address.ToString();
            _nameServerPort = localEndpoint.Port;
        }

        /// <summary>
        /// Returns a stage with the default settings (default name and failure machine).
        /// </summary>
        /// <returns>A stage with default settings</returns>
        public IElasticStage DefaultStage()
        {
            lock (_subsLock)
            {
                IElasticStage defaultStage;
                _stages.TryGetValue(_defaultStageName, out defaultStage);

                if (defaultStage == null)
                {
                    CreateNewStage(_defaultStageName, _numEvaluators, _defaultFailureMachine.Clone(_numEvaluators, (int)DefaultFailureStates.Fail));
                }

                return _stages[_defaultStageName];
            }
        }

        /// <summary>
        /// Creates a new stage.
        ///  The stage lifecicle is managed by the context.
        /// </summary>
        /// <param name="stageName">The name of the stage</param>
        /// <param name="numTasks">The number of tasks required by the stage</param>
        /// <param name="failureMachine">An optional failure machine governing the stage</param>
        /// <returns>The new task Set subscrption</returns>
        public IElasticStage CreateNewStage(
            string stageName,
            int numTasks,
            IFailureStateMachine failureMachine = null)
        {
            if (string.IsNullOrEmpty(stageName))
            {
                throw new ArgumentNullException($"{nameof(stageName)} cannot be null.");
            }

            if (numTasks <= 0)
            {
                throw new ArgumentException($"{nameof(numTasks)} is required to be greater than 0.");
            }

            lock (_subsLock)
            {
                if (_stages.ContainsKey(stageName))
                {
                    throw new ArgumentException($"Stage {stageName} already registered with the context.");
                }

                var stage = new DefaultElasticStage(
                    stageName,
                    numTasks,
                    this,
                    failureMachine ?? _defaultFailureMachine.Clone(numTasks, (int)DefaultFailureStates.Fail));
                _stages[stageName] = stage;

                return stage;
            }
        }

        /// <summary>
        /// Remove a task Set stage from the context.
        /// </summary>
        /// <param name="stageName">The name of the stage to be removed</param>
        public void RemoveElasticStage(string stageName)
        {
            lock (_subsLock)
            {
                if (!_stages.ContainsKey(stageName))
                {
                    throw new ArgumentException($"Stage {stageName} is not registered with the context.");
                }

                _stages.Remove(stageName);
            }
        }

        /// <summary>
        /// Generate the base configuration module for tasks. 
        /// This method is method can be used to generate configurations for the task set menager.
        /// </summary>
        /// <param name="taskId">The id of the task the configuration is generate for</param>
        /// <returns>The module with the service properly set up for the task</returns>
        public ConfigurationModule GetTaskConfigurationModule(string taskId)
        {
            return TaskConfiguration.ConfigurationModule
                    .Set(TaskConfiguration.Identifier, taskId)
                    .Set(TaskConfiguration.OnMessage, GenericType<ElasticDriverMessageHandler>.Class)
                    .Set(TaskConfiguration.OnClose, GenericType<Task.Impl.DefaultElasticContext>.Class);
        }

        /// <summary>
        /// Start the elastic group communication context.
        /// This will trigger requests for resources as specified by the parameters.
        /// </summary>
        public void Start()
        {
            var request = _evaluatorRequestor.NewBuilder()
                .SetNumber(_numEvaluators)
                .SetMegabytes(_memory)
                .SetCores(_cores)
                .SetRackName(_rackName)
                .SetEvaluatorBatchId(_batchId)
                .Build();

            _evaluatorRequestor.Submit(request);
        }

        /// <summary>
        /// Create a new task set manager.
        /// </summary>
        /// <param name="masterTaskConfiguration">The configuration for the master task</param>
        /// <param name="slaveTaskConfiguration">The configuration for the slave task</param>
        /// <returns>A new task set manager</returns>

        public IElasticTaskSetManager CreateNewTaskSetManager(Func<string, IConfiguration> masterTaskConfiguration, Func<string, IConfiguration> slaveTaskConfiguration = null)
        {
            return CreateNewTaskSetManager(_numEvaluators, masterTaskConfiguration, slaveTaskConfiguration);
        }

        /// <summary>
        /// Create a new task set manager.
        /// </summary>
        /// <param name="numOfTasks">The number of tasks the task set should manager</param>
        /// <param name="masterTaskConfiguration">The configuration for the master task</param>
        /// <param name="slaveTaskConfiguration">The configuration for the slave task</param>
        /// <returns>A new task set manager</returns>
        public IElasticTaskSetManager CreateNewTaskSetManager(int numOfTasks, Func<string, IConfiguration> masterTaskConfiguration, Func<string, IConfiguration> slaveTaskConfiguration = null)
        {
            return new DefaultElasticTaskSetManager(numOfTasks, _evaluatorRequestor, _driverId, masterTaskConfiguration, slaveTaskConfiguration);
        }

        /// <summary>
        /// Generate the elastic service configuration object.
        /// This method is used to properly configure task contexts with the elastic service.
        /// </summary>
        /// <returns>The ealstic service configuration</returns>
        public IConfiguration GetElasticServiceConfiguration()
        {
            IConfiguration contextConfig = ServiceConfiguration.ConfigurationModule
                .Set(ServiceConfiguration.Services,
                    GenericType<StreamingNetworkService<ElasticGroupCommunicationMessage>>.Class)
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(contextConfig)
                .BindNamedParameter<NamingConfigurationOptions.NameServerAddress, string>(
                    GenericType<NamingConfigurationOptions.NameServerAddress>.Class,
                    _nameServerAddr)
                .BindNamedParameter<NamingConfigurationOptions.NameServerPort, int>(
                    GenericType<NamingConfigurationOptions.NameServerPort>.Class,
                    _nameServerPort.ToString(CultureInfo.InvariantCulture))
                .BindImplementation(GenericType<INameClient>.Class,
                    GenericType<NameClient>.Class)
                .BindNamedParameter<TcpPortRangeStart, int>(GenericType<TcpPortRangeStart>.Class,
                    _startingPort.ToString(CultureInfo.InvariantCulture))
                .BindNamedParameter<TcpPortRangeCount, int>(GenericType<TcpPortRangeCount>.Class,
                    _portRange.ToString(CultureInfo.InvariantCulture))
                .Build();
        }

        /// <summary>
        /// Appends a stage configuration to a configuration builder object.
        /// </summary>
        /// <param name="confBuilder">The configuration where the stage configuration will be appended to</param>
        /// <param name="stageConf">The stage configuration at hand</param>
        /// <returns>The configuration containing the serialized stage configuration</returns>
        public void SerializeStageConfiguration(ref ICsConfigurationBuilder confBuilder, IConfiguration stageConfiguration)
        {
            confBuilder.BindSetEntry<ElasticServiceConfigurationOptions.SerializedStageConfigs, string>(
                GenericType<ElasticServiceConfigurationOptions.SerializedStageConfigs>.Class,
                _configSerializer.ToString(stageConfiguration));
        }

        /// <summary>
        /// Append an operator configuration to a configuration builder object.
        /// </summary>
        /// <param name="serializedOperatorsConfs">The list where the operator configuration will be appended to</param>
        /// <param name="stageConf">The operator configuration at hand</param>
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
        /// <param name="nextTimeouts">The next timeouts to be scheduled</param>
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
        }

        #endregion

        #region Default Failure event Response
        /// <summary>
        /// Mechanism to execute when a reconfigure event is triggered.
        /// <paramref name="reconfigureEvent"/>
        /// </summary>
        public void OnReconfigure(ref ReconfigureEvent reconfigureEvent)
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
        public void OnReschedule(ref RescheduleEvent rescheduleEvent)
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
        public void OnStop(ref StopEvent stopEvent)
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
