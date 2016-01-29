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
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Runtime.Evaluator.Context;
using Org.Apache.REEF.Common.Runtime.Evaluator.Parameters;
using Org.Apache.REEF.Common.Runtime.Evaluator.Utils;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Time;
using Org.Apache.REEF.Wake.Time.Runtime;

namespace Org.Apache.REEF.Common.Runtime.Evaluator
{
    internal sealed class EvaluatorSettings
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(EvaluatorSettings));

        private readonly string _applicationId;
        private readonly string _evaluatorId;
        private readonly string _rootContextId;
        private readonly int _heartBeatPeriodInMs;
        private readonly int _maxHeartbeatRetries;
        private readonly IClock _clock;
        private readonly IRemoteManager<REEFMessage> _remoteManager;
        private readonly IInjector _injector;
        private readonly IConfiguration _rootContextConfig;
        private readonly AvroConfigurationSerializer _serializer;
        private readonly Optional<TaskConfiguration> _rootTaskConfiguration;
        private readonly Optional<ServiceConfiguration> _rootServiceConfiguration;

        private EvaluatorOperationState _operationState;
        private INameClient _nameClient;

        /// <summary>
        /// Constructor with
        /// </summary>
        /// <param name="applicationId"></param>
        /// <param name="evaluatorId"></param>
        /// <param name="heartbeatPeriodInMs"></param>
        /// <param name="maxHeartbeatRetries"></param>
        /// <param name="rootContextConfigString"></param>
        /// <param name="serializer"></param>
        /// <param name="clock"></param>
        /// <param name="remoteManagerFactory"></param>
        /// <param name="reefMessageCodec"></param>
        /// <param name="injector"></param>
        [Inject]
        private EvaluatorSettings(
            [Parameter(typeof(ApplicationIdentifier))] string applicationId,
            [Parameter(typeof(EvaluatorIdentifier))] string evaluatorId,
            [Parameter(typeof(EvaluatorHeartbeatPeriodInMs))] int heartbeatPeriodInMs,
            [Parameter(typeof(HeartbeatMaxRetry))] int maxHeartbeatRetries,
            [Parameter(typeof(RootContextConfiguration))] string rootContextConfigString,
            AvroConfigurationSerializer serializer,
            RuntimeClock clock,
            IRemoteManagerFactory remoteManagerFactory,
            REEFMessageCodec reefMessageCodec,
            IInjector injector)
        {
            _serializer = serializer;
            _injector = injector;
            _applicationId = applicationId;
            _evaluatorId = evaluatorId;
            _heartBeatPeriodInMs = heartbeatPeriodInMs;
            _maxHeartbeatRetries = maxHeartbeatRetries;
            _clock = clock;

            if (string.IsNullOrWhiteSpace(rootContextConfigString))
            {
                Utilities.Diagnostics.Exceptions.Throw(
                    new ArgumentException("empty or null rootContextConfigString"), Logger);
            }
            _rootContextConfig = _serializer.FromString(rootContextConfigString);

            try
            {
                _rootContextId = injector.ForkInjector(_rootContextConfig).GetNamedInstance<ContextConfigurationOptions.ContextIdentifier, string>();
            }
            catch (InjectionException)
            {
                Logger.Log(Level.Info, "Using deprecated ContextConfiguration.");
                
                // TODO[JIRA REEF-1167]: Remove this catch.
                var deprecatedContextConfig = new Context.ContextConfiguration(rootContextConfigString);
                _rootContextConfig = deprecatedContextConfig;
                _rootContextId = deprecatedContextConfig.Id;
            }

            _rootTaskConfiguration = CreateTaskConfiguration();
            _rootServiceConfiguration = CreateRootServiceConfiguration();

            _remoteManager = remoteManagerFactory.GetInstance(reefMessageCodec);
            _operationState = EvaluatorOperationState.OPERATIONAL;
        }

        /// <summary>
        /// Operator State. Can be set and get.
        /// </summary>
        public EvaluatorOperationState OperationState
        {
            get
            {
                return _operationState;
            }

            set
            {
                _operationState = value;
            }
        }

        /// <summary>
        /// Return Evaluator Id got from Evaluator Configuration
        /// </summary>
        public string EvalutorId
        {
            get
            {
                return _evaluatorId;
            }
        }

        /// <summary>
        /// Returns the root context ID.
        /// </summary>
        public string RootContextId
        {
            get { return _rootContextId; }
        }

        /// <summary>
        /// Return HeartBeatPeriodInMs from NamedParameter
        /// </summary>
        public int HeartBeatPeriodInMs
        {
            get
            {
                return _heartBeatPeriodInMs;
            }
        }

        /// <summary>
        /// Return Application Id got from Evaluator Configuration
        /// </summary>
        public string ApplicationId
        {
            get
            {
                return _applicationId;
            }
        }

        /// <summary>
        /// Return MaxHeartbeatRetries from NamedParameter
        /// </summary>
        public int MaxHeartbeatRetries
        {
            get
            {
                return _maxHeartbeatRetries;
            }
        }

        /// <summary>
        /// return Root Context Configuration passed from Evaluator configuration
        /// </summary>
        public IConfiguration RootContextConfig
        {
            get { return _rootContextConfig; }
        }

        /// <summary>
        /// return Root Task Configuration passed from Evaluator configuration
        /// </summary>
        public Optional<TaskConfiguration> RootTaskConfiguration
        {
            get { return _rootTaskConfiguration; }
        }

        /// <summary>
        /// return Root Service Configuration passed from Evaluator configuration
        /// </summary>
        public Optional<ServiceConfiguration> RootServiceConfiguration
        {
            get { return _rootServiceConfiguration; }
        }

        /// <summary>
        /// return Runtime Clock injected from the constructor
        /// </summary>
        public IClock RuntimeClock
        {
            get
            {
                return _clock;
            }
        }

        /// <summary>
        /// Return Name Client
        /// </summary>
        public INameClient NameClient
        {
            get
            {
                return _nameClient;
            }

            set
            {
                _nameClient = value;
            }
        }

        /// <summary>
        /// return Remote manager
        /// </summary>
        public IRemoteManager<REEFMessage> RemoteManager
        {
            get
            {
                return _remoteManager;
            }
        }

        /// <summary>
        /// Injector that contains clrDrive configuration and Evaluator configuration
        /// </summary>
        public IInjector EvaluatorInjector
        {
            get
            {
                return _injector;
            }
        }

        private Optional<TaskConfiguration> CreateTaskConfiguration()
        {
            string taskConfigString = null;
            try
            {
                taskConfigString = _injector.GetNamedInstance<InitialTaskConfiguration, string>();
            }
            catch (InjectionException)
            {
                Logger.Log(Level.Info, "InitialTaskConfiguration is not set in Evaluator.config.");
            }
            return string.IsNullOrEmpty(taskConfigString)
                ? Optional<TaskConfiguration>.Empty()
                : Optional<TaskConfiguration>.Of(
                    new TaskConfiguration(taskConfigString));
        }

        private Optional<ServiceConfiguration> CreateRootServiceConfiguration()
        {
            string rootServiceConfigString = null;
            try
            {
                rootServiceConfigString = _injector.GetNamedInstance<RootServiceConfiguration, string>();
            }
            catch (InjectionException)
            {
                Logger.Log(Level.Info, "RootServiceConfiguration is not set in Evaluator.config.");
            }
            return string.IsNullOrEmpty(rootServiceConfigString)
                ? Optional<ServiceConfiguration>.Empty()
                : Optional<ServiceConfiguration>.Of(
                    new ServiceConfiguration(rootServiceConfigString));
        }
    }
}
