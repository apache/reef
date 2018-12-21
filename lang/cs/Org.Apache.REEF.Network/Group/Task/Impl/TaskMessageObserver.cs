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
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    /// <summary>
    /// The observer for a Task that multiplexes to the node observers associated with that Task.
    /// </summary>
    internal sealed class TaskMessageObserver :
        IObserver<NsMessage<GeneralGroupCommunicationMessage>>, 
        IObserver<IRemoteMessage<NsMessage<GeneralGroupCommunicationMessage>>>
    {
        private readonly Dictionary<NodeObserverIdentifier, IObserver<NsMessage<GeneralGroupCommunicationMessage>>> _observers =
            new Dictionary<NodeObserverIdentifier, IObserver<NsMessage<GeneralGroupCommunicationMessage>>>();

        private readonly StreamingNetworkService<GeneralGroupCommunicationMessage> _networkService;
        private readonly object _registrationLock = new object();
        private bool _hasRegistered = false;
        private volatile NsMessage<GeneralGroupCommunicationMessage> _registrationMessage;

        public TaskMessageObserver(StreamingNetworkService<GeneralGroupCommunicationMessage> networkService)
        {
            _networkService = networkService;
        }

        /// <summary>
        /// Registers a node associated with the Task.
        /// </summary>
        public void RegisterNodeObserver<T>(NodeMessageObserver<T> observer)
        {
            _observers.Add(NodeObserverIdentifier.FromObserver(observer), observer);
        }

        /// <summary>
        /// This is called directly from the observer container with the registered IPEndpoint
        /// of the Task ID.
        /// </summary>
        public void OnNext(NsMessage<GeneralGroupCommunicationMessage> value)
        {
            Handle(value);
        }

        /// <summary>
        /// This is called from the universal observer in ObserverContainer for the first message.
        /// </summary>
        public void OnNext(IRemoteMessage<NsMessage<GeneralGroupCommunicationMessage>> value)
        {
            // Lock to prevent duplication of messages.
            lock (_registrationLock)
            {
                if (_hasRegistered)
                {
                    return;
                }

                var socketRemoteId = value.Identifier as SocketRemoteIdentifier;
                if (socketRemoteId == null)
                {
                    throw new InvalidOperationException();
                }

                // Handle the message first, then register the observer.
                Handle(value.Message, true);
                _networkService.RemoteManager.RegisterObserver(socketRemoteId.Addr, this);
                _hasRegistered = true;
            }
        }

        public void OnError(Exception error)
        {
            // TODO[JIRA REEF-1407]: Propagate Exception to nodes associated with the Task.
            throw new NotImplementedException();
        }

        public void OnCompleted()
        {
            // TODO[JIRA REEF-1407]: Propagate completion to nodes associated with the Task.
            throw new NotImplementedException();
        }

        /// <summary>
        /// Handles the group communication message.
        /// </summary>
        private void Handle(NsMessage<GeneralGroupCommunicationMessage> value, bool isRegistration = false)
        {
            // This is mainly used to handle the case should ObserverContainer
            // decide to trigger handlers concurrently for a single message.
            if (isRegistration)
            {
                // Process the registration message
                _registrationMessage = value;
            }
            else if (_registrationMessage != null && value == _registrationMessage)
            {
                // This means that we've already processed the message.
                // Ignore this message and discard the reference.
                _registrationMessage = null;
                return;
            }

            var gcMessage = value.Data;

            IObserver<NsMessage<GeneralGroupCommunicationMessage>> observer;
            if (!_observers.TryGetValue(NodeObserverIdentifier.FromMessage(gcMessage), out observer))
            {
                throw new InvalidOperationException();
            }

            observer.OnNext(value);
        }
    }
}