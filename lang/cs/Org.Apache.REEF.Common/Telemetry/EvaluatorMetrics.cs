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
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Telemetry
{
    /// <summary>
    /// An evaluator metrics implementation that maintains a collection of metrics.
    /// </summary>
    internal sealed class EvaluatorMetrics : IEvaluatorMetrics
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(EvaluatorMetrics));

        private readonly MetricsData _metrics;

        [Inject]
        private EvaluatorMetrics(MetricsData metrics)
        {
            _metrics = metrics;
        }

        /// <summary>
        /// Create an EvaluatorMetrics from a serialized metrics string.
        /// </summary>
        /// <param name="serializedMsg"></param>
        internal EvaluatorMetrics(string serializedMsg)
        {
            _metrics = new MetricsData(serializedMsg);
        }

        /// <summary>
        /// Returns metrics
        /// </summary>
        /// <returns>Returns metrics.</returns>
        public MetricsData GetMetricsData()
        {
            return _metrics;
        }

        /// <summary>
        /// return serialized string of metrics metrics data
        /// </summary>
        /// <returns>Returns serialized string of metrics.</returns>
        public string Serialize()
        {
            if (_metrics != null)
            {
                return _metrics.SerializeAndReset();
            }
            return null;
        }
    }
}
