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
using System.Reflection;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Wake.RX.Impl
{
    /// <summary>
    /// Subject to provide publish/subscribe interface.
    /// Subscribes to class Types and invokes handlers for a given
    /// type on call to OnNext
    /// </summary>
    /// <typeparam name="T">The super type that all event types
    /// inherit from</typeparam>
    public class PubSubSubject<T> : IObserver<T>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(PubSubSubject<T>));

        private readonly Dictionary<Type, List<object>> _classToObserversMap;
        private bool _completed;
        private readonly object _mutex;

        /// <summary>
        /// Constructs a pub-sub Subject
        /// </summary>
        public PubSubSubject()
        {
            _classToObserversMap = new Dictionary<Type, List<object>>();
            _mutex = new object();
        }

        /// <summary>
        /// Log on completion
        /// </summary>
        public void OnCompleted()
        {
            lock (_mutex)
            {
                _completed = true;
            }
        }

        /// <summary>
        /// Log Exception
        /// </summary>
        /// <param name="error"></param>
        public void OnError(Exception error)
        {
            lock (_mutex)
            {
                _completed = true;
            }
        }

        /// <summary>
        /// Invoke the subscribed handlers for the event class type
        /// </summary>
        /// <param name="value">The event to process</param>
        public void OnNext(T value)
        {
            if (value == null)
            {
                Exceptions.Throw(new ArgumentNullException("value"), LOGGER);
            }

            lock (_mutex)
            {
                // If OnCompleted or OnError called, do nothing
                if (_completed)
                {
                    return;
                }

                // Check that the event type has been subscribed
                List<object> handlers;
                if (!_classToObserversMap.TryGetValue(value.GetType(), out handlers))
                {
                    Exceptions.Throw(new ArgumentException("No event for type " + value.GetType()), LOGGER);
                }

                // Invoke each IObserver for the event type
                foreach (object handler in handlers)
                {
                    Type handlerType = typeof(IObserver<>).MakeGenericType(new[] { value.GetType() });
                    MethodInfo info = handlerType.GetMethod("OnNext");
                    info.Invoke(handler, new[] { (object)value });
                }
            }
        }

        /// <summary>
        /// Subscribe an IObserver for an event type
        /// </summary>
        /// <typeparam name="U">The event type</typeparam>
        /// <param name="observer">The observer to handle the event</param>
        /// <returns>An IDisposable object used to handle unsubscribing
        /// the IObserver</returns>
        public IDisposable Subscribe<U>(IObserver<U> observer) where U : T
        {
            lock (_mutex)
            {
                List<object> observers;
                if (!_classToObserversMap.TryGetValue(typeof(U), out observers))
                {
                    observers = new List<object>();
                    _classToObserversMap[typeof(U)] = observers;
                }
                observers.Add(observer);
            }

            return new DisposableResource<U>(_classToObserversMap, observer, _mutex);
        }

        /// <summary>
        /// Utility class to handle disposing of an IObserver
        /// </summary>
        private class DisposableResource<U> : IDisposable
        {
            private readonly Dictionary<Type, List<object>> _observersMap;
            private readonly IObserver<U> _observer;
            private readonly object _mutex;
            private bool _disposed;
            
            public DisposableResource(Dictionary<Type, List<object>> observersMap, IObserver<U> observer, object mutex)
            {
                _observersMap = observersMap;
                _observer = observer;
                _mutex = mutex;
                _disposed = false;
            }

            /// <summary>
            /// Unsubscribe the IObserver from the observer map
            /// </summary>
            public void Dispose()
            {
                if (!_disposed)
                {
                    UnsubscribeObserver();
                    _disposed = true;
                }
            }

            private void UnsubscribeObserver()
            {
                lock (_mutex)
                {
                    List<object> observers;
                    if (_observersMap.TryGetValue(typeof(U), out observers))
                    {
                        observers.Remove(_observer);
                    }
                }
            }
        }
    }
}
