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

using Org.Apache.REEF.Common.Metrics.Api;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Common.Metrics.MutableMetricsLayer
{
    /// <summary>
    /// Default implementation of <see cref="IMetricsFactory"/>.
    /// </summary>
    internal sealed class DefaultMetricsFactoryImpl : IMetricsFactory
    {
        [Inject]
        private DefaultMetricsFactoryImpl()
        {    
        }

        /// <summary>
        /// Creates new tag
        /// </summary>
        /// <param name="info">Meta-data of the tag.</param>
        /// <param name="value">Value of the tag.</param>
        /// <returns>The new tag.</returns>
        public MetricsTag CreateTag(IMetricsInfo info, string value)
        {
            return new MetricsTag(info, value);
        }

        /// <summary>
        /// Creates a counter by name. Description is assumed to be 
        /// same as name. Initial value is assumed to be zero.
        /// </summary>
        /// <param name="name">Name of the counter.</param>
        /// <returns>Newly created counter.</returns>
        public ICounter CreateCounter(string name)
        {
            return new MutableCounter(name);
        }

        /// <summary>
        /// Creates a counter by name and description. Initial value is assumed to be zero.
        /// </summary>
        /// <param name="name">Name of the counter.</param>
        /// <param name="desc">Description of the counter</param>
        /// <param name="initValue">Initial value of the counter</param>
        /// <returns>Newly created counter.</returns>
        public ICounter CreateCounter(string name, string desc, long initValue = 0)
        {
            return new MutableCounter(new MetricsInfoImpl(name, desc), initValue);
        }

        /// <summary>
        /// Creates a long gauge by name. Description is assumed to be 
        /// same as name. Initial value is assumed to be zero.
        /// </summary>
        /// <param name="name">Name of the gauge.</param>
        /// <returns>Newly created gauge.</returns>
        public ILongGauge CreateLongGauge(string name)
        {
            return new MutableLongGauge(name);
        }

        /// <summary>
        /// Creates a long gauge by name and description.
        /// </summary>
        /// <param name="name">Name of the gauge.</param>
        /// <param name="desc">Description of the gauge</param>
        /// <param name="initValue">Initial value of the gauge</param>
        /// <returns>Newly created gauge.</returns>
        public ILongGauge CreateLongGauge(string name, string desc, long initValue = 0)
        {
            return new MutableLongGauge(new MetricsInfoImpl(name, desc), initValue);
        }

        /// <summary>
        /// Creates a double gauge by name. Description is assumed to be 
        /// same as name. Initial value is assumed to be zero.
        /// </summary>
        /// <param name="name">Name of the gauge.</param>
        /// <returns>Newly created gauge.</returns>
        public IDoubleGauge CreateDoubleGauge(string name)
        {
            return new MutableDoubleGauge(name);
        }

        /// <summary>
        /// Creates a double gauge by name and description.
        /// </summary>
        /// <param name="name">Name of the gauge.</param>
        /// <param name="desc">Description of the gauge</param>
        /// <param name="initValue">Initial value of the gauge</param>
        /// <returns>Newly created gauge.</returns>
        public IDoubleGauge CreateDoubleGauge(string name, string desc, double initValue = 0)
        {
            return new MutableDoubleGauge(new MetricsInfoImpl(name, desc), initValue);
        }

        /// <summary>
        /// Creates the rate metric by name and description.
        /// </summary>
        /// <param name="name">Name of the rate</param>
        /// <param name="desc">Description of the rate.</param>
        /// <param name="extendedMetrics">if true, stdev, min, max are also generated. Otherwise 
        /// only mean is computed.</param>
        /// <returns>Newly created rate</returns>
        public IRate CreateRateMetric(string name, string desc, bool extendedMetrics = true)
        {
            return new MutableRate(new MetricsInfoImpl(name, desc), extendedMetrics);
        }

        /// <summary>
        /// Creates the rate metric by name. Description is assumed to be 
        /// same as name. All metrics - mean, stdev, min , max are generated.
        /// </summary>
        /// <param name="name">Name of the rate</param>
        /// <returns>Newly created rate</returns>
        public IRate CreateRateMetric(string name)
        {
            return new MutableRate(name);
        }

        /// <summary>
        /// Creates stats metric by name. Description is assumed to be 
        /// same as name. All metrics - mean, stdev, min , max are generated.
        /// </summary>
        /// <param name="name">Name of the rate</param>
        /// <param name="valueName">Value that which this metric represents (for example, Time, Latency etc.</param>
        /// <returns>Newly created stat.</returns>
        public IStat CreateStatMetric(string name, string valueName)
        {
            return new MutableStat(name, valueName);
        }

        /// <summary>
        /// Creates the stat metric by name and description. 
        /// </summary>
        /// <param name="name">Name of the rate</param>
        /// <param name="desc">Description of the rate.</param>
        /// <param name="valueName">Value that which this metric represents (for example, Time, Latency etc.</param>
        /// <param name="extendedMetrics">if true, stdev, min, max are also generated. Otherwise 
        /// only mean is computed.</param>
        /// <returns>Newly created stat.</returns>
        public IStat CreateStatMetric(string name, string desc, string valueName, bool extendedMetrics = true)
        {
            return new MutableStat(new MetricsInfoImpl(name, desc), valueName, extendedMetrics);
        }
    }
}
