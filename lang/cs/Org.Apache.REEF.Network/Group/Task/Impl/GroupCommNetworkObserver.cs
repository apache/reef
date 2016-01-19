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
using System.Linq;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    /// <summary>
    /// Handles all incoming messages for this Task.
    /// Writable version
    /// </summary>
    internal sealed class GroupCommNetworkObserver : IGroupCommNetworkObserver
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(GroupCommNetworkObserver));

        private readonly Dictionary<string, IObserver<GeneralGroupCommunicationMessage>> _commGroupHandlers;

        /// <summary>
        /// Creates a new GroupCommNetworkObserver.
        /// </summary>
        [Inject]
        private GroupCommNetworkObserver()
        {
            _commGroupHandlers = new Dictionary<string, IObserver<GeneralGroupCommunicationMessage>>();
        }

        /// <summary>
        /// Handles the incoming WritableNsMessage for this Task.
        /// Delegates the GeneralGroupCommunicationMessage to the correct 
        /// WritableCommunicationGroupNetworkObserver.
        /// </summary>
        /// <param name="nsMessage"></param>
        public void OnNext(NsMessage<GeneralGroupCommunicationMessage> nsMessage)
        {
            if (nsMessage == null)
            {
                throw new ArgumentNullException("nsMessage");
            }

            try
            {
                GeneralGroupCommunicationMessage gcm = nsMessage.Data.First();
                _commGroupHandlers[gcm.GroupName].OnNext(gcm);
            }
            catch (InvalidOperationException)
            {
                LOGGER.Log(Level.Error, "Group Communication Network Handler received message with no data");
                throw;
            }
            catch (KeyNotFoundException)
            {
                LOGGER.Log(Level.Error, "Group Communication Network Handler received message for nonexistant group");
                throw;
            }
        }

        /// <summary>
        /// Registers the network handler for the given CommunicationGroup.
        /// When messages are sent to the specified group name, the given handler
        /// will be invoked with that message.
        /// </summary>
        /// <param name="groupName">The group name for the network handler</param>
        /// <param name="commGroupHandler">The network handler to invoke when
        /// messages are sent to the given group.</param>
        public void Register(string groupName, IObserver<GeneralGroupCommunicationMessage> commGroupHandler)
        {
            if (string.IsNullOrEmpty(groupName))
            {
                throw new ArgumentNullException("groupName");
            }
            if (commGroupHandler == null)
            {
                throw new ArgumentNullException("commGroupHandler");
            }

            _commGroupHandlers[groupName] = commGroupHandler;
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }
}
