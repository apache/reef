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
using Xunit;

namespace Org.Apache.REEF.Common.Tests.Metrics
{
    /// <summary>
    /// Tests various functionalities of <see cref="ImmutableMetricsImpl"/>
    /// </summary>
    public sealed class ImmutableMetricTest
    {
        /// <summary>
        /// Tests properties for numeric value in <see cref="ImmutableMetricsImpl"/>
        /// </summary>
        [Fact]
        public void TestNumericImmutableMetricProperties()
        {
            const double dValue = 5.0;
            const string name = "testname";
            const string desc = "testdesc";
            IMetricsInfo info = new MetricsInfoImpl(name, desc);

            ImmutableMetricsImpl metric = new ImmutableMetricsImpl(info,
                dValue,
                MetricType.Counter,
                (metricsVisitor) => { });
            Assert.Equal(metric.Info.Name, name);
            Assert.Equal(metric.Info.Description, desc);
            Assert.False(metric.LongValue != null);
            Assert.Equal(metric.NumericValue, dValue);
            Assert.Equal(metric.TypeOfMetric, MetricType.Counter);
        }

        /// <summary>
        /// Tests properties for long value in <see cref="ImmutableMetricsImpl"/>
        /// </summary>
        [Fact]
        public void TestLongImmutableMetricProperties()
        {
            const long lValue = 6;
            const string name = "testname";
            const string desc = "testdesc";
            IMetricsInfo info = new MetricsInfoImpl(name, desc);

            var metric = new ImmutableMetricsImpl(info, lValue, MetricType.Counter, (metricsVisitor) => { });
            Assert.Equal(metric.Info.Name, name);
            Assert.Equal(metric.Info.Description, desc);
            Assert.False(metric.NumericValue != null);
            Assert.Equal(metric.LongValue, lValue);
            Assert.Equal(metric.TypeOfMetric, MetricType.Counter);
        }

        /// <summary>
        /// Tests Equality function for numeric value in <see cref="ImmutableMetricsImpl"/>
        /// </summary>
        [Fact]
        public void TestNumericImmutableMetricEqualityFunction()
        {
            const double dValue = 5.0;
            const long lValue = 6;
            const string name = "testname";
            const string desc = "testdesc";
            IMetricsInfo info = new MetricsInfoImpl(name, desc);

            ImmutableMetricsImpl metric = new ImmutableMetricsImpl(info,
                dValue,
                MetricType.Counter,
                (metricsVisitor) => { });

            ImmutableMetricsImpl otherMetric = new ImmutableMetricsImpl(info,
                dValue,
                MetricType.Counter,
                (metricsVisitor) => { });
            Assert.True(metric.Equals(otherMetric));
            otherMetric = new ImmutableMetricsImpl(info, lValue, MetricType.Counter, (metricsVisitor) => { });
            Assert.False(metric.Equals(otherMetric));
            otherMetric = new ImmutableMetricsImpl(new MetricsInfoImpl("wrongname", desc),
                dValue,
                MetricType.Counter,
                (metricsVisitor) => { });
            Assert.False(metric.Equals(otherMetric));
            otherMetric = new ImmutableMetricsImpl(new MetricsInfoImpl(name, "wrongdesc"),
                dValue,
                MetricType.Counter,
                (metricsVisitor) => { });
            Assert.False(metric.Equals(otherMetric));
            otherMetric = new ImmutableMetricsImpl(info, dValue, MetricType.Gauge, (metricsVisitor) => { });
            Assert.False(metric.Equals(otherMetric));
        }

        /// <summary>
        /// Tests Equality function for long value in <see cref="ImmutableMetricsImpl"/>
        /// </summary>
        [Fact]
        public void TestLongImmutableMetricEqualityFunction()
        {
            const double dValue = 5.0;
            const long lValue = 6;
            const string name = "testname";
            const string desc = "testdesc";
            IMetricsInfo info = new MetricsInfoImpl(name, desc);

            ImmutableMetricsImpl metric = new ImmutableMetricsImpl(info,
                lValue,
                MetricType.Counter,
                (metricsVisitor) => { });

            var otherMetric = new ImmutableMetricsImpl(info, lValue, MetricType.Counter, (metricsVisitor) => { });
            otherMetric = new ImmutableMetricsImpl(info, dValue, MetricType.Counter, (metricsVisitor) => { });
            Assert.False(metric.Equals(otherMetric));
            
            otherMetric = new ImmutableMetricsImpl(new MetricsInfoImpl("wrongname", desc),
                lValue,
                MetricType.Counter,
                (metricsVisitor) => { });
            Assert.False(metric.Equals(otherMetric));
            
            otherMetric = new ImmutableMetricsImpl(new MetricsInfoImpl(name, "wrongdesc"),
                lValue,
                MetricType.Counter,
                (metricsVisitor) => { });
            Assert.False(metric.Equals(otherMetric));
            
            otherMetric = new ImmutableMetricsImpl(info, lValue, MetricType.Gauge, (metricsVisitor) => { });
            Assert.False(metric.Equals(otherMetric));
        }

        /// <summary>
        /// Tests ToString function for numeric value in <see cref="ImmutableMetricsImpl"/>
        /// </summary>
        [Fact]
        public void TestNumericImmutableMetricToStringFunction()
        {
            const double dValue = 5.0;
            const string name = "testname";
            const string desc = "testdesc";
            IMetricsInfo info = new MetricsInfoImpl(name, desc);
            string expectedDoubleString = string.Format("Metric Type: {0}, Metric Information: {1}, Metric Value: {2}",
                MetricType.Counter,
                info,
                dValue);

            ImmutableMetricsImpl metric = new ImmutableMetricsImpl(info,
                dValue,
                MetricType.Counter,
                (metricsVisitor) => { });
            Assert.Equal(metric.ToString(), expectedDoubleString);
        }

        /// <summary>
        /// Tests ToString function for long value in <see cref="ImmutableMetricsImpl"/>
        /// </summary>
        [Fact]
        public void TestLongImmutableMetricToStringFunction()
        {
            const long lValue = 6;
            const string name = "testname";
            const string desc = "testdesc";
            IMetricsInfo info = new MetricsInfoImpl(name, desc);

            string expectedLongString = string.Format("Metric Type: {0}, Metric Information: {1}, Metric Value: {2}",
                MetricType.Counter,
                info,
                lValue);

            ImmutableMetricsImpl metric = new ImmutableMetricsImpl(info,
                lValue,
                MetricType.Counter,
                (metricsVisitor) => { });
            Assert.Equal(metric.ToString(), expectedLongString);
        }
    }
}
