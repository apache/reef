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
    /// Immutable metric interface. All the metrics put in the <see cref="IMetricsRecord"/> 
    /// by <see cref="IMetricsSource"/> are kept as <see cref="IImmutableMetric"/>.
    /// </summary>
    [Unstable("0.16", "Contract may change.")]
    public interface IImmutableMetric
    {
        /// <summary>
        /// Long Value of the metric. Immutable metrics of 
        /// type integrals, byte, bool are all type casted to long before storing
        /// them as immutable metrics. For a given instance of this interface, either this property 
        /// or <see cref="NumericValue"/> returns a valid value.
        /// </summary>
        long? LongValue { get; }

        /// <summary>
        /// Numeric Value of the metric. Immutable metrics of 
        /// non integral numerical types are all type casted to double before storing
        /// them as immutable metrics. For a given instance of this interface, either this property 
        /// or <see cref="LongValue"/> returns a valid value.
        /// </summary>
        double? NumericValue { get; }

        /// <summary>
        /// Meta-data of the metric.
        /// </summary>
        IMetricsInfo Info { get; }

        /// <summary>
        /// String representation of a metric for display.
        /// </summary>
        /// <returns>The string representation of the metric.</returns>
        string ToString();

        /// <summary>
        /// Checks whether two metrics are equal. Relies on Equals 
        /// function of <see cref="IMetricsInfo"/> implementations.
        /// </summary>
        /// <param name="obj">Object to compare against.</param>
        /// <returns>True if both represent the same metric.</returns>
        bool Equals(object obj);

        /// <summary>
        /// Return hash code of the metric object. Simply uses the hash of ToString() method.
        /// </summary>
        /// <returns>Hash code.</returns>
        int GetHashCode();

        /// <summary>
        /// Type of metric - counter or gauge. Filled in by exact 
        /// metric type. It is assumed that converting complex 
        /// metrics like Stats and Rates in to immutable ones require 
        /// their decomposition in to multiple instances of <see cref="IImmutableMetric"/>, 
        /// for example: number of samples will be of type counter, mean, variance etc. 
        /// will be of type gauge, etc.
        /// </summary>
        MetricType TypeOfMetric { get; }

        /// <summary>
        /// Accepts a visitor interface. This function is used to get exact 
        /// metric specfic information (for example, if visitor implementation 
        /// wants to determine whether it is a counter or gauge, etc., by making 
        /// the exact implementation of <see cref="IImmutableMetric"/> call the 
        /// appropriate function in <see cref="IMetricsVisitor"/>. 
        /// </summary>
        /// <param name="visitor">Metrics visitor interface.</param>
        void Visit(IMetricsVisitor visitor);
    }
}
