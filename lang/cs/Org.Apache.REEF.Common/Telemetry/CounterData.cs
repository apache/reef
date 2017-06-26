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

using System.Collections.Generic;

namespace Org.Apache.REEF.Common.Telemetry
{
    /// <summary>
    /// This class wraps a Counter object and the increment value since last sink
    /// </summary>
    internal sealed class CounterData
    {
        /// <summary>
        /// Counter object
        /// </summary>
        private ICounter _counter;

        /// <summary>
        /// Counter increment value since last sink
        /// </summary>
        internal int IncrementSinceLastSink { get; private set; }

        /// <summary>
        /// Constructor for CounterData
        /// </summary>
        /// <param name="counter"></param>
        /// <param name="initialValue"></param>
        internal CounterData(ICounter counter, int initialValue)
        {
            _counter = counter;
            IncrementSinceLastSink = initialValue;
        }

        /// <summary>
        /// clear the increment since last sink
        /// </summary>
        internal void ResetSinceLastSink()
        {
            IncrementSinceLastSink = 0;
        }

        internal void UpdateCounter(ICounter counter)
        {
            IncrementSinceLastSink += counter.Value - _counter.Value;

            //// TODO: [REEF-1748] The following cases need to be considered in determine how to update the counter:
            //// if evaluator contains the aggregated values, the value will override existing value
            //// if evaluator only keep delta, the value should be added at here. But the value in the evaluator should be reset after message is sent
            //// For the counters from multiple evaluators with the same counter name, the value should be aggregated here
            //// We also need to consider failure cases.  
            _counter = counter;
        }

        /// <summary>
        /// Get count name and value as KeyValuePair
        /// </summary>
        /// <returns></returns>
        internal KeyValuePair<string, string> GetKeyValuePair()
        {
            return new KeyValuePair<string, string>(_counter.Name, _counter.Value.ToString());
        }
    }
}
