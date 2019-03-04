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
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Wake.Remote.Impl;
using Org.Apache.REEF.Network.Elastic.Config;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Utilities.Attributes;
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    /// <summary>
    /// Default implementation of the task-side context.
    /// Used by REEF tasks to initialize group communication and fetch stages.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal sealed class DefaultElasticContext : IElasticContext
    {
        private readonly Dictionary<string, IElasticStage> _stages = new Dictionary<string, IElasticStage>();
        private readonly string _taskId;

        private readonly INetworkService<ElasticGroupCommunicationMessage> _networkService;

        private readonly object _disposeLock = new object();
        private bool _disposed = false;

        /// <summary>
        /// Creates a new elastic context and registers the task id with the Name Server.
        /// </summary>
        /// <param name="stageConfigs">The set of serialized stages configurations</param>
        /// <param name="taskId">The identifier for this task</param>
        /// <param name="networkService">The writable network service used to send messages</param>
        /// <param name="configSerializer">Used to deserialize service configuration</param>
        /// <param name="injector">Dependency injector</param>
        [Inject]
        public DefaultElasticContext(
            [Parameter(typeof(ElasticServiceConfigurationOptions.SerializedStageConfigs))] ISet<string> stageConfigs,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            StreamingNetworkService<ElasticGroupCommunicationMessage> networkService,
            AvroConfigurationSerializer configSerializer,
            DefaultTaskToDriverMessageDispatcher taskToDriverDispatcher, // Otherwise the correct instance does not propagate through
            ElasticDriverMessageHandler driverMessageHandler,
            IInjector injector)
        {
            _networkService = networkService;
            _taskId = taskId;

            _stages = stageConfigs
                .Select(config => 
                    injector.ForkInjector(configSerializer.FromString(config)).GetInstance<IElasticStage>())
                .ToDictionary(stage => stage.StageName, stage => stage);

            _networkService.Register(new StringIdentifier(_taskId));
        }

        /// <summary>
        /// This is to ensure all the nodes in the groups are registered before starting communications.
        /// </summary>
        /// <param name="cancellationSource">The token used to signal if the operation got cancelled</param>
        public void WaitForTaskRegistration(CancellationTokenSource cancellationSource = null)
        {
            foreach (var stage in _stages.Values)
            {
                stage.WaitForTaskRegistration(cancellationSource);
            }
        }

        /// <summary>
        /// Gets the stage object for the given stage name.
        /// </summary>
        /// <param name="stageName">The name of the stage</param>
        /// <returns>The task-side stage object</returns>
        public IElasticStage GetStage(string stageName)
        {
            if (!_stages.TryGetValue(stageName, out IElasticStage stage))
            {
                return stage;
            }

            throw new ArgumentException($"No stage with name: {stageName}.");
        }

        /// <summary>
        /// Disposes the services.
        /// </summary>
        public void Dispose()
        {
            lock (_disposeLock)
            {
                if (!_disposed)
                {
                    foreach (var sub in _stages.Values)
                    {
                        sub.Dispose();
                    }

                    _networkService.Unregister();

                    _disposed = true;
                }
            }
        }

        /// <summary>
        /// Action to trigger in case a <see cref="ICloseEvent"/> is received.
        /// </summary>
        /// <param name="value">The close event</param>
        public void OnNext(ICloseEvent value)
        {
            foreach (var stage in _stages.Values)
            {
                stage.Cancel();
            }
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }
}