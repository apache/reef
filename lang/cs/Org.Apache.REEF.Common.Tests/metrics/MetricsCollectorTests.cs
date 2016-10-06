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
using System.Linq;
using Org.Apache.REEF.Common.Metrics.Api;
using Org.Apache.REEF.Common.Metrics.MetricsSystem;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Common.Tests.Metrics
{
    /// <summary>
    /// Tests for <see cref="MetricsCollector"/>, <see cref="MetricsRecord"/> 
    /// and <see cref="MetricsRecordBuilder"/>.
    /// </summary>
    public sealed class MetricsCollectorTests
    {
        /// <summary>
        /// Tests <see cref="MetricsRecord"/>
        /// </summary>
        [Fact]
        public void TestMetricsRecord()
        {
            const string name = "recname";
            const string desc = "rec desc";
            const long timeStamp = 1000;
            const string context = "context";
            const string counterName = "counter";
            const long counterValue = 2;
            const string gaugeName = "gauge";
            const double gaugeValue = 3.0;
            const string tagName = "tagName";
            const string tagValue = "tagValue";

            IList<IImmutableMetric> metrics = new List<IImmutableMetric>();
            metrics.Add(new ImmutableCounter(new MetricsInfoImpl(counterName, counterName), counterValue));
            metrics.Add(new ImmutableDoubleGauge(new MetricsInfoImpl(gaugeName, gaugeName), gaugeValue));

            IList<MetricsTag> tags = new List<MetricsTag>();
            tags.Add(new MetricsTag(new MetricsInfoImpl(tagName, tagName), tagValue));

            MetricsInfoImpl info = new MetricsInfoImpl(name, desc);
            MetricsRecord record = new MetricsRecord(info, timeStamp, metrics, tags, context);

            Assert.Equal(name, record.Name);
            Assert.Equal(desc, record.Description);
            Assert.Equal(context, record.Context);
            Assert.Equal(timeStamp, record.Timestamp);
            Assert.Equal(metrics, record.Metrics);
            Assert.Equal(tags, record.Tags);
        }

        /// <summary>
        /// Tests <see cref="MetricsRecordBuilder"/>
        /// </summary>
        [Fact]
        public void TestMetricsRecordBuilder()
        {
            const string name = "collName";
            const string desc = "coll desc";
            const string context = "context";
            IList<string> counterName = new List<string>() { "counter1", "counter2" };
            IList<long> counterValue = new List<long>() { 2, 6 };
            const string doubleGaugeName = "gauge";
            const double doubleGaugeValue = 3.0;
            const string longGaugeName = "gauge";
            const long longGaugeValue = 4;
            IList<string> tagName = new List<string>() { "tagName1", "tagName2", "tagName3" };
            IList<string> tagValue = new List<string>() { "tagValue1", "tagValue2", "tagValue3" };
            
            var collector = new MetricTestUtils.MetricsCollectorTestImpl();
            MetricsRecordBuilder rb = new MetricsRecordBuilder(collector, new MetricsInfoImpl(name, desc));
            
            rb.AddCounter(new MetricsInfoImpl(counterName[0], counterName[0]), counterValue[0])
                .Add(new ImmutableCounter(new MetricsInfoImpl(counterName[1], counterName[1]), counterValue[1]))
                .AddGauge(new MetricsInfoImpl(longGaugeName, longGaugeName), longGaugeValue)
                .AddGauge(new MetricsInfoImpl(doubleGaugeName, doubleGaugeName), doubleGaugeValue)
                .AddTag(tagName[0], tagValue[0])
                .AddTag(new MetricsInfoImpl(tagName[1], "tagdesc"), tagValue[1])
                .Add(new MetricsTag(new MetricsInfoImpl(tagName[2], tagName[2]), tagValue[2]))
                .SetContext(context)
                .EndRecord();

            TimeSpan t = DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1));
            long expectedTimeStamp = (long)t.TotalSeconds;
            
            var record = rb.GetRecord();
            Assert.Equal(name, record.Name);
            Assert.Equal(desc, record.Description);
            Assert.True(record.Timestamp >= expectedTimeStamp);
            Assert.Equal(context, record.Context);

            int counter = 0;
            foreach (var tag in record.Tags)
            {
                if (counter < tagName.Count)
                {
                    Assert.Equal(tagName[counter], tag.Name);
                    Assert.Equal(tagValue[counter], tag.Value);
                }
                else
                {
                    Assert.Equal(MetricsSystemConstants.Context, tag.Name);
                    Assert.Equal(context, tag.Value);
                }
                counter++;
            }

            counter = 0;
            foreach (var metric in record.Metrics)
            {
                if (counter < 2)
                {
                    Assert.Equal(MetricType.Counter, metric.TypeOfMetric);
                    Assert.Equal(counterName[counter], metric.Info.Name);
                    Assert.Equal(counterValue[counter], metric.LongValue);
                }
                else
                {
                    Assert.Equal(MetricType.Gauge, metric.TypeOfMetric);

                    if (counter == 2)
                    {
                        Assert.Equal(longGaugeName, metric.Info.Name);
                        Assert.Equal(longGaugeValue, metric.LongValue);
                    }
                    else
                    {
                        Assert.Equal(doubleGaugeName, metric.Info.Name);
                        Assert.Equal(doubleGaugeValue, metric.NumericValue);   
                    }
                }
                counter++;
            }
        }

        /// <summary>
        /// Tests <see cref="MetricsCollector"/>
        /// </summary>
        [Fact]
        public void TestMetricsCollector()
        {
            IList<string> recNames = new List<string>() { "recName1", "recName2" };
            const string counterName = "counter";
            const long counterValue = 2;
            const string doubleGaugeName = "gauge";
            const double doubleGaugeValue = 3.0;

            var collector =
                TangFactory.GetTang()
                    .NewInjector(
                        TangFactory.GetTang()
                            .NewConfigurationBuilder()
                            .BindImplementation(GenericType<IMetricsCollector>.Class,
                                GenericType<MetricsCollector>.Class).Build())
                    .GetInstance<IMetricsCollector>();

            collector.CreateRecord(recNames[0])
                .AddCounter(new MetricsInfoImpl(counterName, counterName), counterValue)
                .EndRecord()
                .CreateRecord(new MetricsInfoImpl(recNames[1], recNames[1]))
                .AddGauge(new MetricsInfoImpl(doubleGaugeName, doubleGaugeName), doubleGaugeValue)
                .EndRecord();

            var records = collector.GetRecords();
            Assert.Equal(recNames.Count, records.Count());

            int counter = 0;
            foreach (var record in records)
            {
                Assert.Equal(recNames[counter], record.Name);
                if (counter == 0)
                {
                    Assert.Equal(counterValue, record.Metrics.First().LongValue);
                }
                else
                {
                    Assert.Equal(doubleGaugeValue, record.Metrics.First().NumericValue);
                }
                counter++;
            }

            collector.Clear();
            Assert.Equal(0, collector.GetRecords().Count());
        }
    }
}
