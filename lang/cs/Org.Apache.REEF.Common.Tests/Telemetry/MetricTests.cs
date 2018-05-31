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
using Org.Apache.REEF.Common.Telemetry;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

using IntegerMetric = Org.Apache.REEF.Common.Telemetry.MetricBase<int>;
using DoubleMetric = Org.Apache.REEF.Common.Telemetry.MetricBase<double>;

namespace Org.Apache.REEF.Common.Tests.Telemetry
{
    public class MetricsTests
    {
        /// <summary>
        /// Test IMetrics, ICounters and IEvaluatorMetrics API.
        /// </summary>
        [Fact]
        public void TestEvaluatorMetricsCountersOnly()
        {
            var evalMetrics1 = TangFactory.GetTang().NewInjector().GetInstance<IEvaluatorMetrics>();
            var metrics1 = evalMetrics1.GetMetricsData();
            var counter1 = new CounterMetric("counter1", "counter1 description");
            metrics1.TryRegisterMetric(counter1);
            ValidateMetric(metrics1, "counter1", 0);
            for (int i = 0; i < 5; i++)
            {
                counter1.Increment();
            }
            ValidateMetric(metrics1, "counter1", 5);

            var counterStr = metrics1.SerializeAndReset();
            var trackers = metrics1.GetMetrics();
            foreach (var t in trackers)
            {
                Assert.Equal(0, t.GetMetricRecords().Count);
            }

            var evalMetrics2 = new EvaluatorMetrics(counterStr);
            var metrics2 = evalMetrics2.GetMetricsData();
            ValidateMetric(metrics2, "counter1", 5);
        }

        /// <summary>
        /// Tests updating metric value.
        /// </summary>
        [Fact]
        public void TestMetricSetValue()
        {
            var metrics = CreateMetrics();
            var intMetric = new IntegerMetric("IntMetric", "metric of type int", DateTime.Now.Ticks, 0);
            var doubleMetric = new DoubleMetric("DouMetric", "metric of type double", DateTime.Now.Ticks, 0);

            metrics.TryRegisterMetric(intMetric);
            metrics.TryRegisterMetric(doubleMetric);
            ValidateMetric(metrics, "IntMetric", default(int));
            ValidateMetric(metrics, "DouMetric", default(double));

            intMetric.AssignNewValue(3);
            doubleMetric.AssignNewValue(3.0);

            ValidateMetric(metrics, "IntMetric", 3);
            ValidateMetric(metrics, "DouMetric", 3.0);
        }

        /// <summary>
        /// Test TryRegisterCounter with a duplicated counter name
        /// </summary>
        [Fact]
        public void TestDuplicatedNames()
        {
            var metrics = CreateMetrics();
            Assert.True(metrics.TryRegisterMetric(new CounterMetric("metric1", "metric description")));
            Assert.False(metrics.TryRegisterMetric(new CounterMetric("metric1", "duplicate name")));
        }

        [Fact]
        public void TestUpdateMetricWithDifferentType()
        {
            var metrics = CreateMetrics();
            var douMetric = new DoubleMetric("dou", "Metric of type double.");
            metrics.TryRegisterMetric(douMetric);
            Assert.Throws<ApplicationException>(() => douMetric.AssignNewValue(3));
        }

        [Fact]
        public void TestMetricsSimulateHeartbeat()
        {
            var evalMetrics1 = TangFactory.GetTang().NewInjector().GetInstance<IEvaluatorMetrics>();
            var metrics1 = evalMetrics1.GetMetricsData();
            var counter = new CounterMetric("counter", "counter with no records");
            var iter = new IntegerMetric("iteration", "iteration with records");
            metrics1.TryRegisterMetric(counter);
            metrics1.TryRegisterMetric(iter);
            for (int i = 0; i < 5; i++)
            {
                counter.Increment();
                iter.AssignNewValue(i + 1);
            }
            var me1Str = metrics1.Serialize();
            var evalMetrics2 = new EvaluatorMetrics(me1Str);
            var metrics2 = evalMetrics2.GetMetricsData();

            var sink = TangFactory.GetTang().NewInjector().GetInstance<IMetricsSink>();
            sink.Sink(metrics2.GetMetricsHistoryAndReset());

            var trackers = metrics2.GetMetrics();
            foreach (var t in trackers)
            {
                Assert.Equal(0, t.GetMetricRecords().Count);
            }
        }

        private static void ValidateMetric(IMetrics metricSet, string name, object expectedValue)
        {
            metricSet.TryGetValue(name, out IMetric metric);
            Assert.Equal(expectedValue, metric.ValueUntyped);
        }

        private static MetricsData CreateMetrics()
        {
            var m = TangFactory.GetTang().NewInjector().GetInstance<IEvaluatorMetrics>();
            var c = m.GetMetricsData();
            return c;
        }
    }
}
