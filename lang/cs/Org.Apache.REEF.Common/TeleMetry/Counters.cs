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
using Newtonsoft.Json;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Telemetry
{
    public class Counters : ICounters
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(Counters));

        /// <summary>
        /// It contains name and count pairs
        /// </summary>
        private readonly IDictionary<string, ICounter> _counters = new Dictionary<string, ICounter>();

        /// <summary>
        /// The lock for counters
        /// </summary>
        private readonly object _counterLock = new object();

        [Inject]
        private Counters()
        {
        }

        public Counters(string serializedCountersString)
        {
            var c = JsonConvert.DeserializeObject<IEnumerable<Counter>>(serializedCountersString);
            foreach (var ct in c)
            {
                _counters.Add(ct.Name, ct);
            }
        }

        public IEnumerable<ICounter> GetCounters()
        {
            return _counters.Values;
        }

        /// <summary>
        /// Register a new counter with a specified name.
        /// If name does not exist, the counter will be added and true will be returned
        /// Otherwise the counter will be not added and false will be returned. 
        /// </summary>
        /// <param name="name">Counter name</param>
        /// <param name="description">Counter description</param>
        /// <returns>Returns a boolean to indicate if the counter is added.</returns>
        public bool TryRegisterCounter(string name, string description)
        {
            lock (_counterLock)
            {
                if (_counters.ContainsKey(name))
                {
                    Logger.Log(Level.Warning, "The counter [{0}] already exists.", name);
                    return false;
                }
                _counters.Add(name, new Counter(name, description));
            }
            return true;
        }

        /// <summary>
        /// Get counter for a given name
        /// return false if the counter doesn't exist
        /// </summary>
        /// <param name="name">Name of the counter</param>
        /// <param name="value">Value of the counter returned</param>
        /// <returns>Returns a boolean to indicate if the value is found.</returns>
        public bool TryGetValue(string name, out ICounter value)
        {
            lock (_counterLock)
            {
                return _counters.TryGetValue(name, out value);
            }
        }

        /// <summary>
        /// Increase the counter with the given number
        /// </summary>
        /// <param name="name">Name of the counter</param>
        /// <param name="number">number to increase</param>
        public void Increment(string name, int number)
        {
            ICounter counter;
            if (TryGetValue(name, out counter))
            {
                lock (_counterLock)
                {
                    counter.Increment(number);
                }
            }
            else
            {
                Logger.Log(Level.Error, "The counter [{0}]  has not registered.", name);
                throw new ApplicationException("Counter has not registered:" + name);
            }
        }

        /// <summary>
        /// return serialized string of counter data
        /// TODO: [REEF-] use an unique number for the counter name mapping to reduce the data transfer over the wire
        /// TODO: [REEF-] use Avro schema if that can make the serialized string more compact
        /// </summary>
        /// <returns>Returns serialized string of the counters.</returns>
        public string Serialize()
        {
            lock (_counterLock)
            {
                if (_counters.Count > 0)
                {
                    return JsonConvert.SerializeObject(_counters.Values);
                }
                return null;
            }
        }
    }
}
