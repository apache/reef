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
using System.Collections.Generic;
using Org.Apache.REEF.Common.Metrics.Api;
using Xunit;

namespace Org.Apache.REEF.Common.Tests.Metrics
{
    internal static class MetricTestUtils
    {
        /// <summary>
        /// Implementation of <see cref="IMetricsRecordBuilder"/> to test Snapshot functions of 
        /// different Mutable metrics.
        /// </summary>
        internal sealed class RecordBuilderForTests : IMetricsRecordBuilder
        {
            private readonly Dictionary<string, long> _longMetricVals = new Dictionary<string, long>();
            private readonly Dictionary<string, double> _doubleMetricVals = new Dictionary<string, double>();
            private readonly Dictionary<string, string> _tagVals = new Dictionary<string, string>();

            public RecordBuilderForTests()
            {
            }

            public RecordBuilderForTests(string name)
            {
                Name = name;
            }

            public string Name { get; private set; }

            public string Context { get; private set; }

            public IMetricsRecordBuilder AddTag(string name, string value)
            {
                _tagVals[name] = value;
                return this;
            }

            public IMetricsRecordBuilder AddTag(IMetricsInfo info, string value)
            {
                _tagVals[info.Name] = value;
                return this;
            }

            public IMetricsRecordBuilder Add(MetricsTag tag)
            {
                _tagVals[tag.Name] = tag.Value;
                return this;
            }

            public IMetricsRecordBuilder Add(IImmutableMetric metric)
            {
                throw new NotImplementedException();
            }

            public IMetricsRecordBuilder SetContext(string value)
            {
                Context = value;
                return this;
            }

            public IMetricsRecordBuilder AddCounter(IMetricsInfo info, long value)
            {
                _longMetricVals[info.Name] = value;
                return this;
            }

            public IMetricsRecordBuilder AddGauge(IMetricsInfo info, double value)
            {
                _doubleMetricVals[info.Name] = value;
                return this;
            }

            public IMetricsRecordBuilder AddGauge(IMetricsInfo info, long value)
            {
                _longMetricVals[info.Name] = value;
                return this;
            }

            public IMetricsCollector ParentCollector()
            {
                throw new NotImplementedException();
            }

            public IMetricsCollector EndRecord()
            {
                throw new NotImplementedException();
            }

            public void Validate(string name, long expected)
            {
                if (!_longMetricVals.ContainsKey(name))
                {
                    Assert.True(false, "Metric name not present");
                }
                Assert.Equal(expected, _longMetricVals[name]);
            }

            public void Validate(string name, double expected, double delta)
            {
                if (!_doubleMetricVals.ContainsKey(name))
                {
                    Assert.True(false, "Metric name not present");
                }
                Assert.True(Math.Abs(expected - _doubleMetricVals[name]) < delta);
            }

            public void Validate(string tagName, string expectedTagVal)
            {
                if (!_tagVals.ContainsKey(tagName))
                {
                    Assert.True(false, "Tag name not present");
                }
                Assert.Equal(expectedTagVal, _tagVals[tagName]);
            }

            public bool LongKeyPresent(string name)
            {
                return _longMetricVals.ContainsKey(name);
            }

            public bool DoubleKeyPresent(string name)
            {
                return _doubleMetricVals.ContainsKey(name);
            }

            public bool MetricsEmpty()
            {
                return _doubleMetricVals.Count == 0 && _longMetricVals.Count == 0;
            }

            public void Reset()
            {
                _longMetricVals.Clear();
                _doubleMetricVals.Clear();
                _tagVals.Clear();
            }
        }

        /// <summary>
        /// <see cref="IMetricsCollector"/> implementation for test purposes.
        /// </summary>
        internal sealed class MetricsCollectorTestImpl : IMetricsCollector
        {
            public RecordBuilderForTests CurrentRecordBuilder
            {
                get;
                private set;
            }
            public IMetricsRecordBuilder CreateRecord(string name)
            {
                CurrentRecordBuilder = new RecordBuilderForTests(name);
                return CurrentRecordBuilder;
            }

            public IMetricsRecordBuilder CreateRecord(IMetricsInfo info)
            {
                throw new System.NotImplementedException();
            }
        }

        /// <summary>
        /// Metrics visitor implementation.
        /// </summary>
        internal sealed class MetricsVisitorForTests : IMetricsVisitor
        {
            public long CounterValue { get; private set; }

            public long LongGauge { get; private set; }

            public double DoubleGauge { get; private set; }

            public void Gauge(IMetricsInfo info, long value)
            {
                LongGauge = value;
            }

            public void Gauge(IMetricsInfo info, double value)
            {
                DoubleGauge = value;
            }

            public void Counter(IMetricsInfo info, long value)
            {
                CounterValue = value;
            }
        }
    }
}
