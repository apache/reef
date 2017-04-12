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
        internal ICounter CounterObj { get; set; }

        /// <summary>
        /// Counter increment value since last sink
        /// </summary>
        internal int IncrementSinceLastSink { get; set; }

        /// <summary>
        /// Constructor for CounterData
        /// </summary>
        /// <param name="counter"></param>
        /// <param name="initialValue"></param>
        internal CounterData(ICounter counter, int initialValue)
        {
            CounterObj = counter;
            IncrementSinceLastSink = initialValue;
        }

        /// <summary>
        /// clear the increment since last sink
        /// </summary>
        internal void ResetSinceLastSink()
        {
            IncrementSinceLastSink = 0;
        }

        /// <summary>
        /// The value of the counter
        /// </summary>
        internal int CounterValue
        {
            get { return CounterObj.Value; }
        }
    }
}
