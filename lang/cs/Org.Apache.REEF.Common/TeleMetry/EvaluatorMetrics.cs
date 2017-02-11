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

using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Common.Telemetry
{    
    public class EvaluatorMetrics : IEvaluatorMetrics
    {
        private readonly Counters _counters;

        [Inject]
        private EvaluatorMetrics(Counters counters)
        {
            _counters = counters;
        }

        public EvaluatorMetrics(string serializedMsg)
        {
            _counters = new Counters(serializedMsg);
        }

        /// <summary>
        /// Returns counters
        /// </summary>
        /// <returns>Returns counters.</returns>
        public ICounters GetMetricsCounters()
        {
            return _counters;
        }

        /// <summary>
        /// return serialized string of metrics counters data
        /// </summary>
        /// <returns>Returns serialized string of counters.</returns>
        public string Serialize()
        {
            return _counters.Serialize();
        }
    }
}
