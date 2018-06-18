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
            var counter1 = (CounterMetric)evalMetrics1.CreateAndRegisterMetric<CounterMetric, int>("counter1", "Counter1 description", false);
            for (int i = 0; i < 5; i++)
            {
                counter1.Increment();
            }
            var metricsSet = evalMetrics1.GetMetricsData();
            ValidateMetric(metricsSet, "counter1", 5);

            var trackers = metricsSet.GetMetrics();
            foreach (var t in trackers)
            {
                Assert.Equal(1, t.GetMetricRecords().Count);
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
            var evalMetrics1 = TangFactory.GetTang().NewInjector().GetInstance<IEvaluatorMetrics>();
            var intMetric = (IntegerMetric)evalMetrics1.CreateAndRegisterMetric<IntegerMetric, int>("IntMetric", "metric of type int", true);
            var doubleMetric = (DoubleMetric)evalMetrics1.CreateAndRegisterMetric<DoubleMetric, double>("DouMetric", "metric of type double", true);
            var metrics = evalMetrics1.GetMetricsData();
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
            metrics.RegisterMetric(new CounterMetric("metric1", "metric description"));
            Assert.Throws<ArgumentException>(() => metrics.RegisterMetric(new CounterMetric("metric1", "duplicate name")));
        }

        [Fact]
        public void TestUpdateMetricWithDifferentType()
        {
            var metrics = CreateMetrics();
            var douMetric = new DoubleMetric("dou", "Metric of type double.");
            metrics.RegisterMetric(douMetric);
            Assert.Throws<ApplicationException>(() => douMetric.AssignNewValue(3));
        }

        [Fact]
        public void TestMetricsSimulateHeartbeat()
        {
            var evalMetrics1 = TangFactory.GetTang().NewInjector().GetInstance<IEvaluatorMetrics>();
            var metrics1 = evalMetrics1.GetMetricsData();
            var counter = new CounterMetric("counter", "counter with no records");
            var iter = new IntegerMetric("iteration", "iteration with records");
            metrics1.RegisterMetric(counter);
            metrics1.RegisterMetric(iter);
            for (int i = 0; i < 5; i++)
            {
                counter.Increment();
                iter.AssignNewValue(i + 1);
            }
            var me1Str = metrics1.Serialize();
            var evalMetrics2 = new EvaluatorMetrics(me1Str);
            var metrics2 = evalMetrics2.GetMetricsData();

            var sink = TangFactory.GetTang().NewInjector().GetInstance<IMetricsSink>();
            sink.Sink(metrics2.FlushMetricRecords());

            var trackers = metrics2.GetMetrics();
            foreach (var t in trackers)
            {
                Assert.Equal(1, t.GetMetricRecords().Count);
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
