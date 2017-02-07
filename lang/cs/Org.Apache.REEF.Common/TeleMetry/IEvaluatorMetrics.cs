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
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Telemetry
{
    [Unstable("0.16", "This is to build a simple metrics with counters only. More metrics will be added in future.")]
    [DefaultImplementation(typeof(EvaluatorMetrics))]
    public interface IEvaluatorMetrics
    {
        /// <summary>
        /// Returns metrics counters
        /// </summary>
        /// <returns></returns>
        ICounters GetMetricsCounters();

        /// <summary>
        /// Serialize the metrics data into a string
        /// </summary>
        /// <returns></returns>
        string Serialize();
    }
}