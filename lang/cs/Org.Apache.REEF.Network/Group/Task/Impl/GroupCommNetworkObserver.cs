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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Org.Apache.REEF.Network.Group.Driver.Impl;
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Network.Group.Task.Impl
{
    /// <summary>
    /// Handles all incoming messages for this Task.
    /// Writable version
    /// </summary>
    internal sealed class GroupCommNetworkObserver : IObserver<IRemoteMessage<NsMessage<GeneralGroupCommunicationMessage>>>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(GroupCommNetworkObserver));

        private readonly IInjectionFuture<StreamingNetworkService<GeneralGroupCommunicationMessage>> _networkService;

        private readonly ConcurrentDictionary<string, TaskMessageObserver> _taskMessageObservers =
            new ConcurrentDictionary<string, TaskMessageObserver>();

        /// <summary>
        /// A ConcurrentDictionary is used here since there is no ConcurrentSet implementation in C#, and ConcurrentBag
        /// does not allow for us to check for the existence of an item. The byte is simply a placeholder.
        /// </summary>
        private readonly ConcurrentDictionary<string, byte> _registeredNodes = new ConcurrentDictionary<string, byte>();

        /// <summary>
        /// Creates a new GroupCommNetworkObserver.
        /// </summary>
        [Inject]
        private GroupCommNetworkObserver(
            IInjectionFuture<StreamingNetworkService<GeneralGroupCommunicationMessage>> networkService)
        {
            _networkService = networkService;
        }

        /// <summary>
        /// Registers a <see cref="TaskMessageObserver"/> for a given <see cref="taskSourceId"/>.
        /// If the <see cref="TaskMessageObserver"/> has already been initialized, it will return
        /// the existing one.
        /// </summary>
        public TaskMessageObserver RegisterAndGetForTask(string taskSourceId)
        {
            // Add a TaskMessage observer for each upstream/downstream source.
            return _taskMessageObservers.GetOrAdd(taskSourceId, new TaskMessageObserver(_networkService.Get()));
        }

        /// <summary>
        /// On the first message, we map the <see cref="TaskMessageObserver"/> to the <see cref="IPEndPoint"/>
        /// of the sending Task and register the observer with <see cref="IRemoteManager{T}"/> 
        /// by calling <see cref="TaskMessageObserver#OnNext"/>. On subsequent messages we simply ignore the message
        /// and allow <see cref="ObserverContainer{T}"/> to send the message directly via the <see cref="IPEndPoint"/>.
        /// </summary>
        /// <param name="remoteMessage"></param>
        public void OnNext(IRemoteMessage<NsMessage<GeneralGroupCommunicationMessage>> remoteMessage)
        {
            var nsMessage = remoteMessage.Message;
            var gcm = nsMessage.Data.First();
            var gcMessageTaskSource = gcm.Source;
            TaskMessageObserver observer;
            if (!_taskMessageObservers.TryGetValue(gcMessageTaskSource, out observer))
            {
                throw new KeyNotFoundException("Unable to find registered NodeMessageObserver for source Task " +
                                                gcMessageTaskSource + ".");
            }

            _registeredNodes.GetOrAdd(gcMessageTaskSource,
                id =>
                {
                    observer.OnNext(remoteMessage);
                    return new byte();
                });
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }
}