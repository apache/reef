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
using Newtonsoft.Json;

namespace Org.Apache.REEF.Common.Telemetry
{
    /// <summary>
    /// Counter implementation.
    /// </summary>
    public sealed class CounterMetric : MetricBase<int>, ICounter
    {
        public CounterMetric(string name, string description, bool isImmutable = false)
            : base(name, description, isImmutable)
        {
            _timestamp = DateTime.Now.Ticks;
            _typedValue = default(int);
        }

        [JsonConstructor]
        internal CounterMetric(string name, string description, long timeStamp, int value)
            : base(name, description, timeStamp, value)
        {
            _timestamp = timeStamp;
            _typedValue = value;
        }

        public void Increment(int number = 1)
        {
            _typedValue += number;
            _timestamp = DateTime.Now.Ticks;
            _tracker.OnNext(this);
        }

        public void Decrement(int number = 1)
        {
            _typedValue -= number;
            _timestamp = DateTime.Now.Ticks;
            _tracker.OnNext(this);
        }
    }
}
