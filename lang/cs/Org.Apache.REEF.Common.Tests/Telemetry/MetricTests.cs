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
using System.Linq;
using Org.Apache.REEF.Common.Telemetry;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.Common.Tests.Telemetry
{
    public class MetricsTests
    {
        /// <summary>
        /// Creates and registers a counter metric without tracking to the Evaluator metrics, alters the values, then serializes and deserializes.
        /// </summary>
        [Fact]
        public void TestEvaluatorMetricsCountersOnly()
        {
            var evalMetrics1 = TangFactory.GetTang().NewInjector().GetInstance<IEvaluatorMetrics>();
            var counter1 = evalMetrics1.CreateAndRegisterMetric<CounterMetric>("counter1", "Counter1 description", false);
            for (int i = 0; i < 5; i++)
            {
                counter1.Increment();
            }
            var evalMetricsData = evalMetrics1.GetMetricsData();
            ValidateMetric(evalMetricsData, "counter1", 5);

            foreach (var t in evalMetricsData.GetMetricTrackers())
            {
                Assert.Equal(1, t.GetMetricRecords().ToList().Count);
            }

            var metricsStr = evalMetrics1.Serialize();
            var evalMetrics2 = new EvaluatorMetrics(metricsStr);
            ValidateMetric(evalMetrics2.GetMetricsData(), "counter1", 5);
        }

        /// <summary>
        /// Tests updating metric value.
        /// </summary>
        [Fact]
        public void TestMetricSetValue()
        {
            var evalMetrics = TangFactory.GetTang().NewInjector().GetInstance<IEvaluatorMetrics>();
            var intMetric = evalMetrics.CreateAndRegisterMetric<IntegerMetric>("IntMetric", "metric of type int", true);
            var doubleMetric = evalMetrics.CreateAndRegisterMetric<DoubleMetric>("DouMetric", "metric of type double", true);

            var evalMetricsData = evalMetrics.GetMetricsData();
            ValidateMetric(evalMetricsData, "IntMetric", default(int));
            ValidateMetric(evalMetricsData, "DouMetric", default(double));

            intMetric.AssignNewValue(3);
            doubleMetric.AssignNewValue(3.0);
            ValidateMetric(evalMetricsData, "IntMetric", 3);
            ValidateMetric(evalMetricsData, "DouMetric", 3.0);
        }

        /// <summary>
        /// Test TryRegisterCounter with a duplicated counter name
        /// </summary>
        [Fact]
        public void TestDuplicatedNames()
        {
            var metrics = CreateMetrics();
            metrics.RegisterMetric(new CounterMetric("metric1", "metric description"));
            Assert.Throws<ArgumentException>(() => metrics.RegisterMetric(new CounterMetric("metric1", "duplicate name")));
        }

        [Fact]
        public void TestMetricsSimulateHeartbeat()
        {
            var evalMetrics1 = TangFactory.GetTang().NewInjector().GetInstance<IEvaluatorMetrics>();
            evalMetrics1.CreateAndRegisterMetric<CounterMetric>("counter", "counter with no records", false);
            evalMetrics1.CreateAndRegisterMetric<IntegerMetric>("iteration", "iteration with records", true);
            evalMetrics1.TryGetMetric("counter", out CounterMetric counter);
            evalMetrics1.TryGetMetric("iteration", out IntegerMetric iter);
            for (int i = 0; i < 5; i++)
            {
                counter.Increment();
                iter.AssignNewValue(i + 1);
            }

            var me1Str = evalMetrics1.Serialize();
            var evalMetrics2 = new EvaluatorMetrics(me1Str);
            var metricsData2 = evalMetrics2.GetMetricsData();

            var sink = TangFactory.GetTang().NewInjector().GetInstance<IMetricsSink>();
            sink.Sink(metricsData2.FlushMetricRecords());

            foreach (var t in metricsData2.GetMetricTrackers())
            {
                Assert.Equal(1, t.GetMetricRecords().ToList().Count);
            }
        }

        private static void ValidateMetric(IMetricSet metricSet, string name, object expectedValue)
        {
            Assert.True(metricSet.TryGetMetric(name, out IMetric metric));
            Assert.Equal(expectedValue, metric.ValueUntyped);
        }

        private static IMetricSet CreateMetrics()
        {
            var m = TangFactory.GetTang().NewInjector().GetInstance<IEvaluatorMetrics>();
            var c = m.GetMetricsData();
            return c;
        }
    }
}
