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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Telemetry
{
    internal sealed class CounterMap
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(CounterMap));

        /// <summary>
        /// Registration of counters
        /// </summary>
        private readonly IDictionary<string, CounterData> _counters = new ConcurrentDictionary<string, CounterData>();

        [Inject]
        private CounterMap()
        {            
        }

        /// <summary>
        /// Update counters 
        /// </summary>
        /// <param name="counters"></param>
        internal void Update(ICounters counters)
        {
            foreach (var counter in counters.GetCounters())
            {
                CounterData counterData;
                if (_counters.TryGetValue(counter.Name, out counterData))
                {
                    counterData.UpdateCounter(counter);
                }
                else
                {
                    _counters.Add(counter.Name, new CounterData(counter, counter.Value));
                }

                Logger.Log(Level.Verbose, "Counter name: {0}, value: {1}, description: {2}, time: {3},  incrementSinceLastSink: {4}.",
                    counter.Name, counter.Value, counter.Description, new DateTime(counter.Timestamp), _counters[counter.Name].IncrementSinceLastSink);
            }
        }

        /// <summary>
        /// Reset increment since last sink for each counter
        /// </summary>
        internal void Reset()
        {
            foreach (var c in _counters.Values)
            {
                c.ResetSinceLastSink();
            }
        }

        /// <summary>
        /// Convert the counter data into ISet for sink
        /// </summary>
        /// <returns></returns>
        internal ISet<KeyValuePair<string, string>> GetCounterData()
        {
            var set = new HashSet<KeyValuePair<string, string>>();
            foreach (var c in _counters)
            {
                set.Add(c.Value.GetKeyValuePair());
            }
            return set;
        }

        /// <summary>
        /// The condition that triggers the sink. The condition can be modified later.
        /// </summary>
        /// <returns></returns>
        internal bool TriggerSink(int counterSinkThreshold)
        {
            return _counters.Values.Sum(e => e.IncrementSinceLastSink) > counterSinkThreshold;
        }
    }
}
