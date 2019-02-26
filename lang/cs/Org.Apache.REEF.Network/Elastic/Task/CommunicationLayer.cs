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
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Wake;
using Org.Apache.REEF.Utilities.Logging;
using System.Threading;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Network.Elastic.Comm.Impl;
using Org.Apache.REEF.Network.Elastic.Topology.Physical;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Network.Elastic.Task.Impl
{
    /// <summary>
    /// Handles all incoming / outcoming messages for a given task.
    /// </summary>
    [Unstable("0.16", "API may change")]
    internal abstract class CommunicationLayer :
        IObserver<IRemoteMessage<NsMessage<ElasticGroupCommunicationMessage>>>,
        IDisposable
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(CommunicationLayer));

        private readonly int _timeout;
        private readonly int _retryRegistration;
        private readonly int _retrySending;
        private readonly int _sleepTime;
        private readonly StreamingNetworkService<ElasticGroupCommunicationMessage> _networkService;
        protected readonly DefaultTaskToDriverMessageDispatcher _taskToDriverDispatcher;
        private readonly ElasticDriverMessageHandler _driverMessagesHandler;
        private readonly IIdentifierFactory _idFactory;
        private readonly IDisposable _communicationObserver;
        private readonly ConcurrentDictionary<NodeIdentifier, DriverAwareOperatorTopology> _driverMessageObservers;

        protected bool _disposed = false;

        protected readonly ConcurrentDictionary<NodeIdentifier, IOperatorTopologyWithCommunication> _groupMessageObservers =
            new ConcurrentDictionary<NodeIdentifier, IOperatorTopologyWithCommunication>();

        /// <summary>
        /// Creates a new communication layer.
        /// </summary>
        protected CommunicationLayer(
            int timeout,
            int retryRegistration,
            int sleepTime,
            int retrySending,
            StreamingNetworkService<ElasticGroupCommunicationMessage> networkService,
            DefaultTaskToDriverMessageDispatcher taskToDriverDispatcher,
            ElasticDriverMessageHandler driverMessagesHandler,
            IIdentifierFactory idFactory)
        {
            _timeout = timeout;
            _retryRegistration = retryRegistration;
            _sleepTime = sleepTime;
            _retrySending = retrySending;
            _networkService = networkService;
            _taskToDriverDispatcher = taskToDriverDispatcher;
            _driverMessagesHandler = driverMessagesHandler;
            _idFactory = idFactory;

            _communicationObserver = _networkService.RemoteManager.RegisterObserver(this);
            _driverMessageObservers = _driverMessagesHandler.DriverMessageObservers;
        }

        /// <summary>
        /// Registers a <see cref="IOperatorTopologyWithCommunication"/> with the communication layer.
        /// </summary>
        /// <param name="operatorObserver">The observer of the communicating topology operator</param>
        public void RegisterOperatorTopologyForTask(IOperatorTopologyWithCommunication operatorObserver)
        {
            if (!_groupMessageObservers.TryAdd(operatorObserver.NodeId, operatorObserver))
            {
                throw new IllegalStateException($"Topology for id {operatorObserver.NodeId} already added among listeners.");
            }
        }

        /// <summary>
        /// Registers a <see cref="DriverAwareOperatorTopology"/> with the communication layer.
        /// </summary>
        /// <param name="operatorObserver">The observer of the driver aware topology</param>
        internal void RegisterOperatorTopologyForDriver(DriverAwareOperatorTopology operatorObserver)
        {
            if (!_driverMessageObservers.TryAdd(operatorObserver.NodeId, operatorObserver))
            {
                throw new IllegalStateException($"Topology for id {operatorObserver.NodeId} already added among driver listeners.");
            }
        }

        /// <summary>
        /// Send the communication message to the task whose name is included in the message.
        /// </summary>
        /// <param name="destination">The destination node for the message</param>
        /// <param name="message">The message to send</param>
        /// <param name="cancellationSource">The token to cancel the operation</param>
        internal void Send(
            string destination,
            ElasticGroupCommunicationMessage message,
            CancellationTokenSource cancellationSource)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }
            if (string.IsNullOrEmpty(destination))
            {
                throw new ArgumentNullException(nameof(destination));
            }
            if (_disposed)
            {
                Log.Log(Level.Warning, "Received send message request after disposing: Ignoring.");
                return;
            }

            IIdentifier destId = _idFactory.Create(destination);

            for (int retry = 0;  !Send(destId, message); retry++)
            {
                if (retry > _retrySending)
                {
                    throw new IllegalStateException($"Unable to send message after retrying {retry} times.");
                }
                Thread.Sleep(_timeout);
            }
        }

        /// <summary>
        /// Forward the received message to the target <see cref="IOperatorTopologyWithCommunication"/>.
        /// </summary>
        /// <param name="remoteMessage">The received message</param>
        public abstract void OnNext(IRemoteMessage<NsMessage<ElasticGroupCommunicationMessage>> remoteMessage);

        /// <summary>
        /// Checks if the identifier is registered with the name server.
        /// Throws exception if the operation fails more than the retry count.
        /// </summary>
        /// <param name="identifiers">The identifier to look up</param>
        /// <param name="cancellationSource">The token to cancel the operation</param>
        /// <param name="removed">Nodes that got removed during task registration</param>
        public void WaitForTaskRegistration(
            ICollection<string> identifiers,
            CancellationTokenSource cancellationSource,
            IDictionary<string, byte> removed = null)
        {
            ISet<string> foundSet = new HashSet<string>();

            for (var i = 0; i < _retryRegistration; i++)
            {
                if (cancellationSource != null && cancellationSource.Token.IsCancellationRequested)
                {
                    Log.Log(Level.Warning, "WaitForTaskRegistration is canceled in retryCount {0}.", i);
                    throw new OperationCanceledException("WaitForTaskRegistration is canceled");
                }

                Log.Log(Level.Info, "WaitForTaskRegistration, in retryCount {0}.", i);
                foreach (var identifier in identifiers)
                {
                    var notFound = !foundSet.Contains(identifier);
                    if (notFound && removed != null && removed.ContainsKey(identifier))
                    {
                        foundSet.Add(identifier);
                        Log.Log(Level.Verbose,
                            "WaitForTaskRegistration, dependent id {0} was removed at loop {1}.", identifier, i);
                    }
                    else if (notFound && Lookup(identifier))
                    {
                        foundSet.Add(identifier);
                        Log.Log(Level.Verbose,
                            "WaitForTaskRegistration, find a dependent id {0} at loop {1}.", identifier, i);
                    }
                }

                if (foundSet.Count >= identifiers.Count)
                {
                    Log.Log(Level.Info,
                        "WaitForTaskRegistration, found all {0} dependent ids at loop {1}.", foundSet.Count, i);
                    return;
                }

                Thread.Sleep(_sleepTime);
            }

            var msg = string.Join(",", identifiers.Except(foundSet));

            Log.Log(Level.Error, "Cannot find registered parent/children: {0}.", msg);
            throw new Exception("Failed to find parent/children nodes");
        }

        /// <summary>
        /// Look up an identifier with the name server.
        /// </summary>
        /// <param name="identifier">The identifier to look up</param>
        /// <returns></returns>
        public bool Lookup(string identifier)
        {
            return !_disposed && _networkService?.NamingClient.Lookup(identifier) != null;
        }

        /// <summary>
        /// Remove the connection to the target destination.
        /// </summary>
        /// <param name="destination">The node to remove the connection</param>
        public void RemoveConnection(string destination)
        {
            IIdentifier destId = _idFactory.Create(destination);
            _networkService.RemoveConnection(destId);
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
            foreach (var observer in _groupMessageObservers.Values)
            {
                observer.OnCompleted();
            }
        }

        /// <summary>
        /// Dispose the connection layer.
        /// </summary>
        public void Dispose()
        {
            if (!_disposed)
            {
                OnCompleted();

                _groupMessageObservers.Clear();

                _communicationObserver.Dispose();

                _disposed = true;

                Log.Log(Level.Info, "Communication layer disposed.");
            }
        }

        private bool Send(IIdentifier destId, ElasticGroupCommunicationMessage message)
        {
            var connection = _networkService.NewConnection(destId);

            try
            {
                if (!connection.IsOpen)
                {
                    connection.Open();
                }

                connection.Write(message);
            }
            catch (Exception e)
            {
                Log.Log(Level.Warning, "Unable to send message to " + destId, e.Message);
                connection.Dispose();
                return false;
            }

            return true;
        }
    }
}