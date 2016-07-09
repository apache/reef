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
using Org.Apache.REEF.Common.Metrics.Api;
using Org.Apache.REEF.Common.Metrics.MutableMetricsLayer;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Xunit;

namespace Org.Apache.REEF.Common.Tests.Metrics
{
    /// <summary>
    /// Tests various classes in REEF.Common.metrics.MutableMetricsLib.
    /// </summary>
    public sealed class TestMutableMetrics
    {
        /// <summary>
        /// Tests various functions of <see cref="MutableCounter"/>
        /// </summary>
        [Fact]
        public void TestCounterMetrics()
        {
            const string name = "countertest";
            const string desc = "countertestdesc";
            IMetricsFactory factory = TangFactory.GetTang().NewInjector().GetInstance<IMetricsFactory>();

            var longCounter = factory.CreateCounter(name, desc, 5);
            Assert.Equal(name, longCounter.Info.Name);
            Assert.Equal(desc, longCounter.Info.Description);

            MetricTestUtils.RecordBuilderForTests recordBuilder = new MetricTestUtils.RecordBuilderForTests();
            SnapshotRequest request = new SnapshotRequest(recordBuilder, false);

            longCounter.OnNext(request);
            recordBuilder.Validate(name, 5);
            recordBuilder.Reset();

            longCounter.Increment();
            longCounter.OnNext(request);
            recordBuilder.Validate(name, 6);
            recordBuilder.Reset();

            longCounter.OnNext(request);
            Assert.False(recordBuilder.LongKeyPresent(name), "Metric is not supposed to be recorded.");

            request = new SnapshotRequest(recordBuilder, true);
            longCounter.OnNext(request);
            recordBuilder.Validate(name, 6);
            recordBuilder.Reset();
        }

        /// <summary>
        /// Tests various functions of <see cref="MutableDoubleGauge"/> and <see cref="MutableLongGauge"/>.
        /// </summary>
        [Fact]
        public void TestGaugeMetrics()
        {
            const string name = "gaugetest";
            const string desc = "guagetestdesc";
            IMetricsFactory factory = TangFactory.GetTang().NewInjector().GetInstance<IMetricsFactory>();

            var doubleGauge = factory.CreateDoubleGauge(name, desc, 5);
            Assert.Equal(name, doubleGauge.Info.Name);
            Assert.Equal(desc, doubleGauge.Info.Description);

            MetricTestUtils.RecordBuilderForTests recordBuilder = new MetricTestUtils.RecordBuilderForTests();
            SnapshotRequest request = new SnapshotRequest(recordBuilder, false);

            doubleGauge.OnNext(request);
            recordBuilder.Validate(name, 5.0, 1e-10);
            recordBuilder.Reset();

            doubleGauge.Increment(2.2);
            doubleGauge.OnNext(request);
            recordBuilder.Validate(name, 7.2, 1e-10);
            recordBuilder.Reset();

            doubleGauge.Decrement(1);
            doubleGauge.OnNext(request);
            recordBuilder.Validate(name, 6.2, 1e-10);
            recordBuilder.Reset();

            doubleGauge.OnNext(request);
            Assert.False(recordBuilder.DoubleKeyPresent(name), "Metric is not supposed to be recorded.");

            request = new SnapshotRequest(recordBuilder, true);
            doubleGauge.OnNext(request);
            recordBuilder.Validate(name, 6.2, 1e-10);
            recordBuilder.Reset();

            request = new SnapshotRequest(recordBuilder, false);
            var longGauge = factory.CreateLongGauge(name, desc, 5);
            Assert.Equal(name, longGauge.Info.Name);
            Assert.Equal(desc, longGauge.Info.Description);

            longGauge.OnNext(request);
            recordBuilder.Validate(name, (long)5);
            recordBuilder.Reset();

            longGauge.Increment(2);
            longGauge.OnNext(request);
            recordBuilder.Validate(name, (long)7);
            recordBuilder.Reset();

            longGauge.Decrement(1);
            longGauge.OnNext(request);
            recordBuilder.Validate(name, (long)6);
            recordBuilder.Reset();

            longGauge.OnNext(request);
            Assert.False(recordBuilder.LongKeyPresent(name), "Metric is not supposed to be recorded.");

            request = new SnapshotRequest(recordBuilder, true);
            longGauge.OnNext(request);
            recordBuilder.Validate(name, (long)6);
            recordBuilder.Reset();
        }

        /// <summary>
        /// Tests various functions of <see cref="MutableStat"/>.
        /// </summary>
        [Fact]
        public void TestStatMetrics()
        {
            const string name = "stattest";
            const string desc = "stattestdesc";
            const string valueName = "statValName";
            const double delta = 1e-10;
            IMetricsFactory factory = TangFactory.GetTang().NewInjector().GetInstance<IMetricsFactory>();

            double[] array1 = new double[3];
            double[] array2 = new double[3];
            Random randGen = new Random();

            array1 = array1.Select(x => randGen.NextDouble()).ToArray();
            array2 = array2.Select(x => randGen.NextDouble()).ToArray();

            var stat = factory.CreateStatMetric(name, desc, valueName);
            MetricTestUtils.RecordBuilderForTests recordBuilder = new MetricTestUtils.RecordBuilderForTests();
            var request = new SnapshotRequest(recordBuilder, false);

            foreach (var entry in array1)
            {
                stat.Sample(entry);
            }

            double expectedMean = array1.Sum() / 3;
            double expectedStd = Math.Sqrt(array1.Select(x => Math.Pow(x - expectedMean, 2)).Sum() / 2);

            stat.OnNext(request);
            recordBuilder.Validate(name + "-Num", (long)3);
            recordBuilder.Validate(name + "-RunningAvg", expectedMean, delta);
            recordBuilder.Validate(name + "-RunningStdev", expectedStd, delta);
            recordBuilder.Validate(name + "-IntervalAvg", expectedMean, delta);
            recordBuilder.Validate(name + "-IntervalStdev", expectedStd, delta);
            recordBuilder.Validate(name + "-RunningMin", array1.Min(), delta);
            recordBuilder.Validate(name + "-IntervalMin", array1.Min(), delta);
            recordBuilder.Validate(name + "-RunningMax", array1.Max(), delta);
            recordBuilder.Validate(name + "-IntervalMax", array1.Max(), delta);

            recordBuilder.Reset();
            foreach (var entry in array2)
            {
                stat.Sample(entry);
            }

            double expectedIntervalMean = array2.Sum() / 3;
            double expectedIntervalStd = Math.Sqrt(array2.Select(x => Math.Pow(x - expectedIntervalMean, 2)).Sum() / 2);
            double expectedIntervalMin = array2.Min();
            double expectedIntervalMax = array2.Max();
            double expectedRunningMean = (array1.Sum() + array2.Sum()) / 6;
            double expectedRunningStd =
                Math.Sqrt((array1.Select(x => Math.Pow(x - expectedRunningMean, 2)).Sum() +
                           array2.Select(x => Math.Pow(x - expectedRunningMean, 2)).Sum()) / 5);
            double expectedRunningMin = Math.Min(array1.Min(), array2.Min());
            double expectedRunningMax = Math.Max(array1.Max(), array2.Max());

            stat.OnNext(request);
            recordBuilder.Validate(name + "-Num", (long)6);
            recordBuilder.Validate(name + "-RunningAvg", expectedRunningMean, delta);
            recordBuilder.Validate(name + "-RunningStdev", expectedRunningStd, delta);
            recordBuilder.Validate(name + "-IntervalAvg", expectedIntervalMean, delta);
            recordBuilder.Validate(name + "-IntervalStdev", expectedIntervalStd, delta);
            recordBuilder.Validate(name + "-RunningMin", expectedRunningMin, delta);
            recordBuilder.Validate(name + "-IntervalMin", expectedIntervalMin, delta);
            recordBuilder.Validate(name + "-RunningMax", expectedRunningMax, delta);
            recordBuilder.Validate(name + "-IntervalMax", expectedIntervalMax, delta);

            recordBuilder.Reset();
            stat.OnNext(request);
            Assert.False(recordBuilder.LongKeyPresent(name + "-Num"), "Metric is not supposed to be recorded.");

            request = new SnapshotRequest(recordBuilder, true);
            stat.OnNext(request);
            Assert.True(recordBuilder.LongKeyPresent(name + "-Num"), "Metric is supposed to be recorded.");
        }
    }
}