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

using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Protobuf.ReefProtocol;
using Org.Apache.REEF.Common.Runtime.Evaluator.Parameters;
using Org.Apache.REEF.Common.Runtime.Evaluator.Utils;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
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
        private readonly int _heartBeatPeriodInMs;
        private readonly int _maxHeartbeatRetries;
        private readonly IClock _clock;
        private readonly IRemoteManager<REEFMessage> _remoteManager;

        /// <summary>
        /// Constructor with
        /// </summary>
        /// <param name="applicationId"></param>
        /// <param name="evaluatorId"></param>
        /// <param name="heartbeatPeriodInMs"></param>
        /// <param name="maxHeartbeatRetries"></param>
        /// <param name="clock"></param>
        /// <param name="remoteManagerFactory"></param>
        /// <param name="reefMessageCodec"></param>
        [Inject]
        private EvaluatorSettings(
            [Parameter(typeof(ApplicationIdentifier))] string applicationId,
            [Parameter(typeof(EvaluatorIdentifier))] string evaluatorId,
            [Parameter(typeof(EvaluatorHeartbeatPeriodInMs))] int heartbeatPeriodInMs,
            [Parameter(typeof(HeartbeatMaxRetry))] int maxHeartbeatRetries,
            RuntimeClock clock,
            IRemoteManagerFactory remoteManagerFactory,
            REEFMessageCodec reefMessageCodec) :
            this(applicationId, evaluatorId, heartbeatPeriodInMs, maxHeartbeatRetries, 
            clock, remoteManagerFactory, reefMessageCodec, null)
        {
        }

        [Inject]
        private EvaluatorSettings(
            [Parameter(typeof(ApplicationIdentifier))] string applicationId,
            [Parameter(typeof(EvaluatorIdentifier))] string evaluatorId,
            [Parameter(typeof(EvaluatorHeartbeatPeriodInMs))] int heartbeatPeriodInMs,
            [Parameter(typeof(HeartbeatMaxRetry))] int maxHeartbeatRetries,
            RuntimeClock clock,
            IRemoteManagerFactory remoteManagerFactory,
            REEFMessageCodec reefMessageCodec,
            INameClient nameClient)
        {
            _applicationId = applicationId;
            _evaluatorId = evaluatorId;
            _heartBeatPeriodInMs = heartbeatPeriodInMs;
            _maxHeartbeatRetries = maxHeartbeatRetries;
            _clock = clock;

            _remoteManager = remoteManagerFactory.GetInstance(reefMessageCodec);
            OperationState = EvaluatorOperationState.OPERATIONAL;
            NameClient = nameClient;
        }

        /// <summary>
        /// Operator State. Can be set and get.
        /// </summary>
        public EvaluatorOperationState OperationState { get; set; }

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
        public INameClient NameClient { get; set; }

        /// <summary>
        /// return Remote manager
        /// </summary>
        public IRemoteManager<REEFMessage> RemoteManager
        {
            get { return _remoteManager; }
        }
    }
}
