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
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    /// <summary>
    /// Used by Tasks to fetch CommunicationGroupClients.
    /// Writable version
    /// </summary>
    public sealed class GroupCommClient : IGroupCommClient
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(GroupCommClient));

        private readonly Dictionary<string, ICommunicationGroupClientInternal> _commGroups;

        private readonly INetworkService<GeneralGroupCommunicationMessage> _networkService;

        /// <summary>
        /// Creates a new WritableGroupCommClient and registers the task ID with the Name Server.
        /// </summary>
        /// <param name="groupConfigs">The set of serialized Group Communication configurations</param>
        /// <param name="taskId">The identifier for this task</param>
        /// <param name="networkService">The writable network service used to send messages</param>
        /// <param name="configSerializer">Used to deserialize Group Communication configuration</param>
        /// <param name="injector">injector forked from the injector that creates this instance</param>
        [Inject]
        public GroupCommClient(
            [Parameter(typeof(GroupCommConfigurationOptions.SerializedGroupConfigs))] ISet<string> groupConfigs,
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            StreamingNetworkService<GeneralGroupCommunicationMessage> networkService,
            AvroConfigurationSerializer configSerializer,
            IInjector injector)
        {
            _commGroups = new Dictionary<string, ICommunicationGroupClientInternal>();
            _networkService = networkService;

            foreach (string serializedGroupConfig in groupConfigs)
            {
                IConfiguration groupConfig = configSerializer.FromString(serializedGroupConfig);
                IInjector groupInjector = injector.ForkInjector(groupConfig);
                var commGroupClient = (ICommunicationGroupClientInternal)groupInjector.GetInstance<ICommunicationGroupClient>();
                _commGroups[commGroupClient.GroupName] = commGroupClient;
            }

            networkService.Register(new StringIdentifier(taskId));

            try
            {
                foreach (var group in _commGroups.Values)
                {
                    group.WaitingForRegistration();
                }
            }
            catch (SystemException e)
            {
                networkService.Unregister();
                networkService.Dispose();
                Exceptions.CaughtAndThrow(e, Level.Error, "In GroupCommClient, exception from WaitingForRegistration.", Logger);
            }
        }

        /// <summary>
        /// Gets the CommunicationGroupClient for the given group name.
        /// </summary>
        /// <param name="groupName">The name of the CommunicationGroupClient</param>
        /// <returns>The CommunicationGroupClient</returns>
        public ICommunicationGroupClient GetCommunicationGroup(string groupName)
        {
            if (string.IsNullOrEmpty(groupName))
            {
                throw new ArgumentNullException("groupName");
            }
            if (!_commGroups.ContainsKey(groupName))
            {
                throw new ArgumentException("No CommunicationGroupClient with name: " + groupName);
            }

            return _commGroups[groupName];
        }

        /// <summary>
        /// Disposes of the GroupCommClient's services.
        /// </summary>
        public void Dispose()
        {
            _networkService.Unregister();
            _networkService.Dispose();
        }
    }
}