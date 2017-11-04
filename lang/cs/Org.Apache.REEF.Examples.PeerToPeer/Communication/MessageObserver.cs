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
using Org.Apache.REEF.Network.NetworkService;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Remote;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Examples.PeerToPeer.Communication
{
    class MessageObserver<T> : IObserver<NsMessage<Message<T>>>, IObserver<IRemoteMessage<NsMessage<Message<T>>>>
    {
        private readonly Dictionary<string, IObserver<NsMessage<Message<T>>>> _observers;
        private static readonly Logger Logger = Logger.GetLogger(typeof(MessageObserver<T>));

        [Inject]
        private MessageObserver()
        {
            _observers = new Dictionary<string, IObserver<NsMessage<Message<T>>>>();
        }

        public void RegisterObserver(string destination, IObserver<NsMessage<Message<T>>> observer)
        {
            _observers[destination] = observer;
        }

        /// <summary>
        /// Watch for messages incoming
        /// </summary>
        /// <param name="nsMessage"></param>
        public void OnNext(NsMessage<Message<T>> nsMessage)
        {
            string destination = string.Empty;
            if (nsMessage.Data.Count > 0)
            {
                destination = nsMessage.Data[0].Destination;
            }

            if (!_observers.TryGetValue(destination, out IObserver<NsMessage<Message<T>>> observer))
            {
                Logger.Log(Level.Warning, "Skipping message for {destination}. Listener not registered.");
                return;
            }

            observer.OnNext(nsMessage);
        }

        /// <summary>
        /// Watch for messages incoming
        /// </summary>
        /// <param name="nsMessage"></param>
        public void OnNext(IRemoteMessage<NsMessage<Message<T>>> remoteNsMessage)
        {
            this.OnNext(remoteNsMessage.Message);
        }

        public void OnError(Exception error)
        {
            throw error;
        }

        public void OnCompleted()
        {
        }
    }
}
