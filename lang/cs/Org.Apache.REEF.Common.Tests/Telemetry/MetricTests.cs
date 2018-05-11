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
        /// Test IMetrics, ICounters and IEvaluatorMetrics API.
        /// </summary>
        [Fact]
        public void TestEvaluatorMetricsCountersOnly()
        {
            var evalMetrics1 = TangFactory.GetTang().NewInjector().GetInstance<IEvaluatorMetrics>();
            var metrics1 = evalMetrics1.GetMetricsData();
            metrics1.TryRegisterMetric(new Counter("counter1", "counter1 description"));
            metrics1.TryRegisterMetric(new Counter("counter2", "counter2 description"));
            ValidateMetric(metrics1, "counter1", 0);
            ValidateMetric(metrics1, "counter2", 0);
            for (int i = 0; i < 5; i++)
            {
                metrics1.Update("counter1", i);
                metrics1.Update("counter2", i * 2);
            }
            ValidateMetric(metrics1, "counter1", 4);
            ValidateMetric(metrics1, "counter2", 8);

            var counterStr = metrics1.Serialize();

            var evalMetrics2 = new EvaluatorMetrics(counterStr);
            var metrics2 = evalMetrics2.GetMetricsData();
            ValidateMetric(metrics2, "counter1", 4);
            ValidateMetric(metrics2, "counter2", 8);
        }

        /// <summary>
        /// Tests updating metric value.
        /// </summary>
        [Fact]
        public void TestMetricSetValue()
        {
            var metrics = CreateMetrics();
            metrics.TryRegisterMetric(new IntegerGauge("int1", "metric of type int", DateTime.Now.Ticks, 0));
            metrics.TryRegisterMetric(new DoubleGauge("dou2", "metric of type double", DateTime.Now.Ticks, 0));
            ValidateMetric(metrics, "int1", default(int));
            ValidateMetric(metrics, "dou2", default(double));

            metrics.Update(new IntegerGauge("int1", "new description", DateTime.Now.Ticks, 3));
            metrics.Update("dou2", 3.14);

            ValidateMetric(metrics, "int1", 3);
            ValidateMetric(metrics, "dou2", 3.14);
        }

        /// <summary>
        /// Test TryRegisterCounter with a duplicated counter name
        /// </summary>
        [Fact]
        public void TestDuplicatedNames()
        {
            var metrics = CreateMetrics();
            metrics.TryRegisterMetric(new Counter("metric1", "metric description"));
            Assert.False(metrics.TryRegisterMetric(new Counter("metric1", "duplicate name")));
        }

        [Fact]
        public void TestUpdateMetricWithDifferentType()
        {
            var metrics = CreateMetrics();
            metrics.TryRegisterMetric(new IntegerGauge("int1", "metric of type int", DateTime.Now.Ticks, 0));
            Assert.Throws<ApplicationException>(() => metrics.Update(new DoubleGauge("int1", "new description", DateTime.Now.Ticks, 3)));
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
