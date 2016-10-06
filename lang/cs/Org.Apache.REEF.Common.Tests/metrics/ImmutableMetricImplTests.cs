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
using Org.Apache.REEF.Common.Metrics.MetricsSystem;
using Xunit;

namespace Org.Apache.REEF.Common.Tests.Metrics
{
    /// <summary>
    /// Tests various implementations of <see cref="IImmutableMetric"/>
    /// </summary>
    public sealed class ImmutableMetricImplTests
    {
        /// <summary>
        /// Tests <see cref="ImmutableCounter"/>.
        /// </summary>
        [Fact]
        public void TestImmutableCounter()
        {
            const string name = "default";
            const string desc = "default";
            const long value = 3;
            ImmutableCounter counter = new ImmutableCounter(new MetricsInfoImpl(name, desc), value);
            Assert.Equal(value, counter.LongValue);
            Assert.Null(counter.NumericValue);
            Assert.Equal(MetricType.Counter, counter.TypeOfMetric);
            
            MetricTestUtils.MetricsVisitorForTests visitor = new MetricTestUtils.MetricsVisitorForTests();
            counter.Visit(visitor);
            Assert.Equal(value, visitor.CounterValue);
        }

        /// <summary>
        /// Tests <see cref="ImmutableLongGauge"/>.
        /// </summary>
        [Fact]
        public void TestImmutableLongGauge()
        {
            const string name = "default";
            const string desc = "default";
            const long value = 3;
            var gauge = new ImmutableLongGauge(new MetricsInfoImpl(name, desc), value);
            Assert.Equal(value, gauge.LongValue);
            Assert.Null(gauge.NumericValue);
            Assert.Equal(MetricType.Gauge, gauge.TypeOfMetric);

            MetricTestUtils.MetricsVisitorForTests visitor = new MetricTestUtils.MetricsVisitorForTests();
            gauge.Visit(visitor);
            Assert.Equal(value, visitor.LongGauge);
        }

        /// <summary>
        /// Tests <see cref="ImmutableDoubleGauge"/>
        /// </summary>
        [Fact]
        public void TestImmutableDoubleGauge()
        {
            const string name = "default";
            const string desc = "default";
            const double value = 4;
            var gauge = new ImmutableDoubleGauge(new MetricsInfoImpl(name, desc), value);
            Assert.Equal(value, gauge.NumericValue);
            Assert.Null(gauge.LongValue);
            Assert.Equal(MetricType.Gauge, gauge.TypeOfMetric);

            MetricTestUtils.MetricsVisitorForTests visitor = new MetricTestUtils.MetricsVisitorForTests();
            gauge.Visit(visitor);
            Assert.Equal(value, visitor.DoubleGauge);
        }
    }
}
