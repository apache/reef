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
using System.Net;
using Org.Apache.REEF.Wake.Util;

namespace Org.Apache.REEF.Wake.Remote.Impl
{
    /// <summary>
    /// Stores registered IObservers for DefaultRemoteManager.
    /// Can register and look up IObservers by remote IPEndPoint.
    /// TODO[JIRA REEF-1407]: Remove <see cref="IObserver{T}"/> and add custom OnError/OnCompleted with IPEndpoints.
    /// </summary>
    internal sealed class ObserverContainer<T> : IObserver<TransportEvent<IRemoteEvent<T>>>
    {
        private readonly ConcurrentDictionary<IPEndPoint, IObserver<T>> _endpointMap;
        private IObserver<T> _universalObserver;
        private IObserver<IRemoteMessage<T>> _remoteMessageUniversalObserver;

        /// <summary>
        /// Constructs a new ObserverContainer used to manage remote IObservers.
        /// </summary>
        public ObserverContainer()
        {
            _endpointMap = new ConcurrentDictionary<IPEndPoint, IObserver<T>>(new IPEndPointComparer());
        }

        /// <summary>
        /// Registers an IObserver used to handle incoming messages from the remote host
        /// at the specified IPEndPoint.
        /// </summary>
        /// <param name="remoteEndpoint">The IPEndPoint of the remote host</param>
        /// <param name="observer">The IObserver to handle incoming messages</param>
        /// <returns>An IDisposable used to unregister the observer with</returns>
        public IDisposable RegisterObserver(IPEndPoint remoteEndpoint, IObserver<T> observer)
        {
            if (remoteEndpoint.Address.Equals(IPAddress.Any))
            {
                _universalObserver = observer;
                return Disposable.Create(() => { _universalObserver = null; });
            }

            _endpointMap[remoteEndpoint] = observer;
            return Disposable.Create(() => _endpointMap.TryRemove(remoteEndpoint, out observer));
        }

        /// <summary>
        /// Registers an IObserver to handle incoming messages from a remote host
        /// </summary>
        /// <param name="observer">The IObserver to handle incoming messages</param>
        /// <returns>An IDisposable used to unregister the observer with</returns>
        public IDisposable RegisterObserver(IObserver<IRemoteMessage<T>> observer)
        {
            _remoteMessageUniversalObserver = observer;
            return Disposable.Create(() => _remoteMessageUniversalObserver = null);
        }

        /// <summary>
        /// Look up the IObserver for the registered IPEndPoint or event type 
        /// and execute the IObserver.
        /// </summary>
        /// <param name="transportEvent">The incoming remote event</param>
        public void OnNext(TransportEvent<IRemoteEvent<T>> transportEvent)
        {
            IRemoteEvent<T> remoteEvent = transportEvent.Data;
            remoteEvent.LocalEndPoint = transportEvent.Link.LocalEndpoint;
            remoteEvent.RemoteEndPoint = transportEvent.Link.RemoteEndpoint;
            T value = remoteEvent.Value;
            bool handled = false;

            if (_universalObserver != null)
            {
                _universalObserver.OnNext(value);
                handled = true;
            }

            if (_remoteMessageUniversalObserver != null)
            {
                // IObserver was registered by event type
                IRemoteIdentifier id = new SocketRemoteIdentifier(remoteEvent.RemoteEndPoint);
                IRemoteMessage<T> remoteMessage = new DefaultRemoteMessage<T>(id, value);
                _remoteMessageUniversalObserver.OnNext(remoteMessage);
                handled = true;
            }

            IObserver<T> observer;
            if (_endpointMap.TryGetValue(remoteEvent.RemoteEndPoint, out observer))
            {
                // IObserver was registered by IPEndpoint
                observer.OnNext(value);
                handled = true;
            }

            if (!handled)
            {
                throw new WakeRuntimeException("Unrecognized Wake RemoteEvent message");
            }
        }

        public void OnError(Exception error)
        {
            // TODO[JIRA REEF-1407]: Propagate Exception upwards. May need to change signature.
        }

        public void OnCompleted()
        {
            // TODO[JIRA REEF-1407]: Propagate completion upwards. May need to change signature.
        }
    }
}