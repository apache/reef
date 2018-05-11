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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Telemetry
{
    public interface IMetrics
    {
        /// <summary>
        /// Register a new metric.
        /// </summary>
        /// <param name="metric">The metric to be registered.</param>
        /// <returns>Returns a boolean to indicate if the metric has been added.</returns>
        bool TryRegisterMetric(IMetric metric);

        /// <summary>
        /// Get metric value given the metric name.
        /// </summary>
        /// <param name="name">Name of the metric</param>
        /// <param name="metric">The metric object returned</param>
        /// <returns>Returns a boolean to indicate if the value is found.</returns>
        bool TryGetValue(string name, out IMetric metric);

        /// <summary>
        /// Returns all the metrics.
        /// </summary>
        /// <returns></returns>
        IEnumerable<MetricData> GetMetrics();

        /// <summary>
        /// Serialize the metrics into strings
        /// </summary>
        /// <returns>Returns serialized string of the metrics.</returns>
        string Serialize();
    }
}