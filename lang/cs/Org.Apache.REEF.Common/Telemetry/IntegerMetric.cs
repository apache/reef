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

using System.Threading;
using Newtonsoft.Json;

namespace Org.Apache.REEF.Common.Telemetry
{
    /// <summary>
    /// Integer metric implementation.
    /// </summary>
    public class IntegerMetric : MetricBase<int>
    {
        public IntegerMetric()
        {
        }

        internal IntegerMetric(string name, string description, bool keepHistory = true)
            : base(name, description, keepHistory)
        {
        }

        [JsonConstructor]
        internal IntegerMetric(string name, string description, int value, bool keepUpdateHistory)
            : base(name, description, value, keepUpdateHistory)
        {
        }

        public override void AssignNewValue(int value)
        {
            Interlocked.Exchange(ref _typedValue, value);
            _tracker.Track(value);
        }
    }
}
