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

namespace Org.Apache.REEF.Common.Metrics.Api
{
    /// <summary>
    /// Base implementation of <see cref="IImmutableMetric"/>.
    /// </summary>
    internal class ImmutableMetricsImpl : IImmutableMetric
    {
        private readonly IMetricsInfo _info;
        private readonly Action<IMetricsVisitor> _onVisit;

        /// <summary>
        /// Constructor. Called by metrics of integral types.
        /// </summary>
        /// <param name="info">Meta-data for the metric</param>
        /// <param name="value">The integral value of the metric. Integral types can be 
        /// directly type casted to ulong.</param>
        /// <param name="typeOfMetric">Type of metric - counter or gauge</param>
        /// <param name="onVisit">Action to take on receiving <see cref="IMetricsVisitor"/></param>
        public ImmutableMetricsImpl(IMetricsInfo info,
            long value,
            MetricType typeOfMetric,
            Action<IMetricsVisitor> onVisit)
        {
            LongValue = value;
            NumericValue = null;
            _info = info;
            TypeOfMetric = typeOfMetric;
            _onVisit = onVisit;
        }

        /// <summary>
        /// Constructor. Called by metrics of numerical types.
        /// </summary>
        /// <param name="info">Meta-data for the metric</param>
        /// <param name="value">The numerical value of the metric. Numerical types can be 
        /// directly type casted to double.</param>
        /// <param name="typeOfMetric">Type of metric - counter or gauge</param>
        /// <param name="onVisit">Action to take on receiving <see cref="IMetricsVisitor"/></param>
        public ImmutableMetricsImpl(IMetricsInfo info,
            double value,
            MetricType typeOfMetric,
            Action<IMetricsVisitor> onVisit)
        {
            NumericValue = value;
            LongValue = null;
            TypeOfMetric = typeOfMetric;
            _info = info;
            _onVisit = onVisit;
        }

        /// <summary>
        /// Meta-data of the metric.
        /// </summary>
        public IMetricsInfo Info
        {
            get { return _info; }
        }

        /// <summary>
        /// Long Value of the metric. Immutable metrics of 
        /// type integrals, byte, bool are all type casted to long before storing
        /// them as immutable metrics. For a given instance of this interface, either this property 
        /// or <see cref="NumericValue"/> returns a valid value.
        /// </summary>
        public long? LongValue { get; private set; }

        /// <summary>
        /// Numeric Value of the metric. Immutable metrics of 
        /// non integral numerical types are all type casted to double before storing
        /// them as immutable metrics. For a given instance of this interface, either this property 
        /// or <see cref="LongValue"/> returns a valid value.
        /// </summary>
        public double? NumericValue { get; private set; }

        /// <summary>
        /// Type of metric - counter or gauge. Filled in by exact 
        /// metric type.
        /// </summary>
        public MetricType TypeOfMetric { get; private set; }

        /// <summary>
        /// Accepts a visitor interface
        /// </summary>
        /// <param name="visitor">Metrics visitor interface.</param>
        public void Visit(IMetricsVisitor visitor)
        {
            _onVisit(visitor);
        }

        /// <summary>
        /// String representation of a metric for display.
        /// </summary>
        /// <returns>The string representation of the metric.</returns>
        public override string ToString()
        {
            return string.Format("Metric Type: {0}, Metric Information: {1}, Metric Value: {2}",
                TypeOfMetric,
                _info,
                LongValue ?? NumericValue);
        }

        /// <summary>
        /// Checks whether two metrics are equal. Relies on Equals 
        /// function of <see cref="IMetricsInfo"/> implementations.
        /// </summary>
        /// <param name="obj">Object to compare against.</param>
        /// <returns>True if both represent the same metric.</returns>
        public override bool Equals(object obj)
        {
            var otherMetric = obj as IImmutableMetric;
            if (otherMetric != null)
            {
                if (otherMetric.Info.Equals(_info) && otherMetric.TypeOfMetric.Equals(TypeOfMetric) &&
                    otherMetric.LongValue == LongValue)
                {
                    return LongValue != null
                        ? LongValue.Value == otherMetric.LongValue.Value
                        : NumericValue.Value == otherMetric.NumericValue.Value;
                }
            }
            return false;
        }

        /// <summary>
        /// Return hash code of the metric object. Simply uses the hash of ToString() method.
        /// </summary>
        /// <returns>Hash code.</returns>
        public override int GetHashCode()
        {
            var hashCode = ToString().GetHashCode();
            return hashCode;
        }
    }
}
