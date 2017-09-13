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
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake;
using Org.Apache.REEF.Wake.Remote.Impl;

namespace Org.Apache.REEF.Examples.PeerToPeer.Communication
{
    class Communicator<T> : IObserver<NsMessage<Message<T>>>, IDisposable
    {
        private readonly string _taskId;
        private readonly MessageObserver<T> _messageObserver;
        private StreamingNetworkService<Message<T>> _networkService;
        private readonly IIdentifierFactory _idFactory;
        private readonly Dictionary<string, IIdentifier> _destinationIds;
        private Queue<T> _taskMessageQueue;
        private static readonly Logger Logger = Logger.GetLogger(typeof(Communicator<T>));

        /// <summary>
        /// Shows if the object has been disposed.
        /// </summary>
        private int _disposed;

        [Inject]
        Communicator(
            [Parameter(typeof(TaskConfigurationOptions.Identifier))] string taskId,
            StreamingNetworkService<Message<T>> networkService,
            IIdentifierFactory idFactory,
            MessageObserver<T> messageObserver)
        {
            _taskId = taskId;
            _networkService = networkService;
            _idFactory = idFactory;
            _messageObserver = messageObserver;
            _destinationIds = new Dictionary<string, IIdentifier>();

            // Register to listen to messages
            _networkService.Register(new StringIdentifier(taskId));
            _messageObserver.RegisterObserver(_taskId, this);
        }

        public void RegisterTask(Queue<T> taskMessageQueue)
        {
            _taskMessageQueue = taskMessageQueue;
        }

        public void OnNext(NsMessage<Message<T>> nsMessage)
        {
            if (_taskMessageQueue == null)
            {
                Logger.Log(Level.Warning, "Skipping message for {destination}. Queue not registered.");
                return;
            }

            foreach (var message in nsMessage.Data)
            {
                _taskMessageQueue.Enqueue(message.Data);
            }
        }

        public void Send(string taskIdTo, T data)
        {
            var message = new Message<T>(
                _taskId,
                taskIdTo,
                data);

            // keep a dictionary of connections
            if (!_destinationIds.ContainsKey(taskIdTo))
            {
                _destinationIds[taskIdTo] = _idFactory.Create(message.Destination);
            }

            var conn = _networkService.NewConnection(_destinationIds[taskIdTo]);
            conn.Open();
            conn.Write(message);
        }

        public void OnError(Exception error)
        {
            throw error;
        }

        public void OnCompleted()
        {
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _networkService.Unregister();
                _networkService.Dispose();
            }

            _taskMessageQueue = null;
        }
    }
}
