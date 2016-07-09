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

using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Metrics.Api
{
    /// <summary>
    /// Metrics visitor interface. The visitor is used to extract metric 
    /// specific information from the immutable metrics where specfic information 
    /// is lost. For example, <see cref="IImmutableMetric"/> loses any information about the derived class 
    /// but provides a function that takes IMetricsVisitor as input. The specific 
    /// implementations can then call the appropriate call back functions below 
    /// that then allows visitor to take appropriate action. This interface can for 
    /// example be used by observers of <see cref="IMetricsRecord"/> to get specific information about metrics 
    /// when it receives the record.
    /// </summary>
    [Unstable("0.16", "Contract may change.")]
    public interface IMetricsVisitor
    {
        /// <summary>
        /// Callback for long value gauges
        /// </summary>
        /// <param name="info">Meta-data of the metric.</param>
        /// <param name="value">Long value of the gauge.</param>
        void Gauge(IMetricsInfo info, long value);

        /// <summary>
        /// Callback for double value gauges
        /// </summary>
        /// <param name="info">Meta-data of the metric.</param>
        /// <param name="value">Double value of the gauge.</param>
        void Gauge(IMetricsInfo info, double value);

        /// <summary>
        /// Callback for long value counter
        /// </summary>
        /// <param name="info">Meta-data of the metric.</param>
        /// <param name="value">Long value of the counter.</param>
        void Counter(IMetricsInfo info, long value);
    }
}
