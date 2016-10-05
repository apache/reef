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
using System.Threading;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Io;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Task.Impl;
using Org.Apache.REEF.Network.Naming;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;

namespace Org.Apache.REEF.Network.Group.Driver.Impl
{
    /// <summary>
    /// Used to create Communication Groups for Group Communication Operators on the Reef driver.
    /// Also manages configuration for Group Communication tasks/services.
    /// </summary>
    public sealed class GroupCommDriver : IGroupCommDriver
    {
        private const string MasterTaskContextName = "MasterTaskContext";
        private const string SlaveTaskContextName = "SlaveTaskContext";

        private static readonly Logger Logger = Logger.GetLogger(typeof(GroupCommDriver));

        private readonly string _driverId;
        private readonly string _nameServerAddr;
        private readonly int _nameServerPort;
        private int _contextIds;
        private readonly int _fanOut;
        private readonly string _defaultGroupName;
        private readonly int _defaultGroupNumberOfTasks;

        private readonly Dictionary<string, ICommunicationGroupDriver> _commGroups;
        private readonly AvroConfigurationSerializer _configSerializer;
        private readonly object _groupsLock = new object();

        /// <summary>
        /// Create a new GroupCommunicationDriver object.
        /// </summary>
        /// <param name="driverId">Identifier for the REEF driver</param>
        /// <param name="masterTaskId">Identifier for Group Communication master task</param>
        /// <param name="fanOut">fanOut for tree topology</param>
        /// <param name="defaultGroupName">default communication group name</param>
        /// <param name="defaultNumberOfTasks">Number of tasks in the default group</param>
        /// <param name="configSerializer">Used to serialize task configuration</param>
        /// <param name="nameServer">Used to map names to ip addresses</param>
        [Inject]
        private GroupCommDriver(
            [Parameter(typeof(GroupCommConfigurationOptions.DriverId))] string driverId,
            [Parameter(typeof(GroupCommConfigurationOptions.MasterTaskId))] string masterTaskId,
            [Parameter(typeof(GroupCommConfigurationOptions.FanOut))] int fanOut,
            [Parameter(typeof(GroupCommConfigurationOptions.GroupName))] string defaultGroupName,
            [Parameter(typeof(GroupCommConfigurationOptions.NumberOfTasks))] int defaultNumberOfTasks,
            AvroConfigurationSerializer configSerializer,
            INameServer nameServer)
        {
            _driverId = driverId;
            _contextIds = -1;
            _fanOut = fanOut;
            MasterTaskId = masterTaskId;
            _defaultGroupName = defaultGroupName;
            _defaultGroupNumberOfTasks = defaultNumberOfTasks;

            _configSerializer = configSerializer;
            _commGroups = new Dictionary<string, ICommunicationGroupDriver>();

            IPEndPoint localEndpoint = nameServer.LocalEndpoint;
            _nameServerAddr = localEndpoint.Address.ToString();
            _nameServerPort = localEndpoint.Port;
        }

        /// <summary>
        /// Returns the identifier for the master task
        /// </summary>
        public string MasterTaskId { get; set; }

        public ICommunicationGroupDriver DefaultGroup
        {
            get
            {
                lock (_groupsLock)
                {
                    ICommunicationGroupDriver defaultGroup;
                    _commGroups.TryGetValue(_defaultGroupName, out defaultGroup);

                    if (defaultGroup == null)
                    {
                        NewCommunicationGroup(_defaultGroupName, _defaultGroupNumberOfTasks);
                    }
                    return _commGroups[_defaultGroupName];
                }
            }
        }

        /// <summary>
        /// Create a new CommunicationGroup with the given name and number of tasks/operators. 
        /// </summary>
        /// <param name="groupName">The new group name</param>
        /// <param name="numTasks">The number of tasks/operators in the group.</param>
        /// <returns>The new Communication Group</returns>
        public ICommunicationGroupDriver NewCommunicationGroup(string groupName, int numTasks)
        {
            if (string.IsNullOrEmpty(groupName))
            {
                Exceptions.Throw(new ArgumentNullException("groupName"), Logger);
            }

            if (numTasks < 1)
            {
               Exceptions.Throw(new ArgumentException("NumTasks must be greater than 0"), Logger);
            }

            lock (_groupsLock)
            {
                if (_commGroups.ContainsKey(groupName))
                {
                    Exceptions.Throw(new ArgumentException("Group Name already registered with GroupCommunicationDriver"), Logger);
                }

                var commGroup = new CommunicationGroupDriver(groupName, _driverId, numTasks, _fanOut, _configSerializer);
                _commGroups[groupName] = commGroup;
                return commGroup;
            }
        }

        /// <summary>
        /// Remove a group from the GroupCommDriver
        /// Throw ArgumentException if the group does not exist
        /// </summary>
        /// <param name="groupName"></param>
        public void RemoveCommunicationGroup(string groupName)
        {
            lock (_groupsLock)
            {
                if (!_commGroups.ContainsKey(groupName))
                {
                    Exceptions.Throw(new ArgumentException("Group Name is not registered with GroupCommunicationDriver"), Logger);
                }

                _commGroups.Remove(groupName);
            }
        }

        /// <summary>
        /// Generates context configuration with a unique identifier.
        /// </summary>
        /// <returns>The configured context configuration</returns>
        public IConfiguration GetContextConfiguration()
        {
            int contextNum = Interlocked.Increment(ref _contextIds);
            string id = (contextNum == 0)
                ? MasterTaskContextName
                : GetSlaveTaskContextName(contextNum);

            return ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, id)
                .Build();
        }

        /// <summary>
        /// Get the service configuration required for running Group Communication on Reef tasks.
        /// </summary>
        /// <returns>The service configuration for the Reef tasks</returns>
        public IConfiguration GetServiceConfiguration()
        {
            IConfiguration serviceConfig = ServiceConfiguration.ConfigurationModule
                .Set(ServiceConfiguration.Services, GenericType<StreamingNetworkService<GeneralGroupCommunicationMessage>>.Class)
                .Build();

            return TangFactory.GetTang().NewConfigurationBuilder(serviceConfig)
                .BindImplementation(
                    GenericType<IObserver<IRemoteMessage<NsMessage<GeneralGroupCommunicationMessage>>>>.Class,
                    GenericType<GroupCommNetworkObserver>.Class)
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
        /// Get the configuration for a particular task.  
        /// The task may belong to many Communication Groups, so each one is serialized
        /// in the configuration as a SerializedGroupConfig.
        /// The user must merge their part of task configuration (task id, task class)
        /// with this returned Group Communication task configuration.
        /// </summary>
        /// <param name="taskId">The id of the task Configuration to generate</param>
        /// <returns>The Group Communication task configuration with communication group and
        /// operator configuration set.</returns>
        public IConfiguration GetGroupCommTaskConfiguration(string taskId)
        {
            var confBuilder = TangFactory.GetTang().NewConfigurationBuilder();

            foreach (ICommunicationGroupDriver commGroup in _commGroups.Values)
            {
                var taskConf = commGroup.GetGroupTaskConfiguration(taskId);
                if (taskConf != null)
                {
                    confBuilder.BindSetEntry<GroupCommConfigurationOptions.SerializedGroupConfigs, string>(
                        GenericType<GroupCommConfigurationOptions.SerializedGroupConfigs>.Class,
                        _configSerializer.ToString(taskConf));
                }
            }

            return confBuilder.Build();
        }

        /// <summary>
        /// Checks whether this active context can be used to run the Master Task.
        /// </summary>
        /// <param name="activeContext">The active context to check</param>
        /// <returns>True if the active context can run the Master task,
        /// otherwise false.</returns>
        public bool IsMasterTaskContext(IActiveContext activeContext)
        {
            return activeContext.Id.Equals(MasterTaskContextName);
        }

        /// <summary>
        /// Checks whether this context configuration is used to configure the Master Task.
        /// </summary>
        /// <param name="contextConfiguration">The context configuration to check</param>
        /// <returns>True if the context configuration is used to configure the Master
        /// Task, otherwise false.</returns>
        public bool IsMasterContextConfiguration(IConfiguration contextConfiguration)
        {
            return Utilities.Utils.GetContextId(contextConfiguration).Equals(MasterTaskContextName);
        }

        /// <summary>
        /// Gets the context number associated with the Active Context id.
        /// </summary>
        /// <param name="activeContext">The active context to check</param>
        /// <returns>The context number associated with the active context id</returns>
        public int GetContextNum(IActiveContext activeContext)
        {
            if (activeContext.Id.Equals(MasterTaskContextName))
            {
                return 0;
            }

            string[] parts = activeContext.Id.Split('-');
            if (parts.Length != 2)
            {
                Exceptions.Throw(new ArgumentException("Invalid id in active context"), Logger);
            }

            return int.Parse(parts[1], CultureInfo.InvariantCulture);
        }

        private string GetSlaveTaskContextName(int contextNum)
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}-{1}", SlaveTaskContextName, contextNum);
        }
    }
}
