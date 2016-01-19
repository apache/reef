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
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Wake.Impl
{
    /// <summary>
    /// Event handler to provide publish/subscribe interfaces
    /// </summary>
    /// <typeparam name="T">The type of event handler</typeparam>
    public class PubSubEventHandler<T> : IEventHandler<T>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(PubSubEventHandler<T>));

        private readonly Dictionary<Type, List<object>> _classToHandlersMap;

        /// <summary>
        /// Construct a pub-sub event handler
        /// </summary>
        public PubSubEventHandler()
        {
            _classToHandlersMap = new Dictionary<Type, List<object>>();
        }

        /// <summary>
        /// Subscribe an event handler for an event type
        /// </summary>
        /// <typeparam name="U">The type of event handler</typeparam>
        /// <param name="handler">The event handler</param>
        public void Subscribe<U>(IEventHandler<U> handler) where U : T
        {
            lock (_classToHandlersMap)
            {
                List<object> handlers;
                if (!_classToHandlersMap.TryGetValue(typeof(U), out handlers))
                {
                    handlers = new List<object>();
                    _classToHandlersMap[typeof(U)] = handlers;
                }
                handlers.Add(handler);
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
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ArgumentNullException("value"), LOGGER);
            }

            lock (_classToHandlersMap)
            {
                // Check that the event type has been subscribed
                List<object> handlers;
                if (!_classToHandlersMap.TryGetValue(value.GetType(), out handlers))
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new ArgumentException("No event for type " + value.GetType()), LOGGER);
                }

                // Invoke each handler for the event type
                foreach (object handler in handlers)
                {
                    Type handlerType = typeof(IEventHandler<>).MakeGenericType(new[] { value.GetType() });
                    MethodInfo info = handlerType.GetMethod("OnNext");
                    info.Invoke(handler, new[] { (object)value });
                }
            }
        }
    }
}