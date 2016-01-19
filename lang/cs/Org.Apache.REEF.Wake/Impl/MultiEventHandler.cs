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
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Wake.Impl
{
    /// <summary>Event handler that dispatches an event to a specific handler based on an event class type
    /// </summary>
    public class MultiEventHandler<T> : IEventHandler<T>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(MultiEventHandler<T>));
        private readonly IDictionary<Type, IEventHandler<T>> _map;

        /// <summary>Constructs a multi-event handler</summary>
        /// <param name="map">a map of class types to event handlers</param>
        public MultiEventHandler(IDictionary<Type, IEventHandler<T>> map)
        {
            foreach (Type item in map.Keys)
            {
                if (!typeof(T).IsAssignableFrom(item))
                {
                    Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new WakeRuntimeException(typeof(T) + " is not assignable from " + item), LOGGER);
                }
            }
            _map = map;
        }

        /// <summary>
        /// Invokes a specific handler for the event class type if it exists
        /// </summary>
        /// <param name="value">The event to handle</param>
        public void OnNext(T value)
        {
            IEventHandler<T> handler = null;
            bool success = _map.TryGetValue(value.GetType(), out handler);
            if (success)
            {
                handler.OnNext(value);
            }
            else
            {
                Org.Apache.REEF.Utilities.Diagnostics.Exceptions.Throw(new WakeRuntimeException("No event " + value.GetType() + " handler"), LOGGER);
            }            
        }
    }
}
