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
using System.Threading;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Common.Metrics.Api;
using Org.Apache.REEF.Common.Metrics.MetricsSystem;
using Org.Apache.REEF.Common.Metrics.MetricsSystem.Parameters;
using Org.Apache.REEF.Common.Metrics.MutableMetricsLayer;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Common.Tests.Metrics
{
    /// <summary>
    /// Tests various functionalities of metircs system.
    /// </summary>
    public class MetricsSystemTests
    {
        /// <summary>
        /// Tests metrics system functioning with single source and sink.
        /// </summary>
        [Fact]
        public void TestMetricSystemFunctioningSingleSourceSingleSink()
        {
            TestMetricSystemFunctioning(1, 1, false);
        }

        /// <summary>
        /// Tests metrics system functioning with two sources and sinks.
        /// </summary>
        [Fact]
        public void TestMetricSystemFunctioningTwoSourceTwoSink()
        {
            TestMetricSystemFunctioning(2, 2, false);
        }

        /// <summary>
        /// Tests metrics system functioning even when unchanged metrics 
        /// are recorded.
        /// </summary>
        [Fact]
        public void TestGettingUnchangedMetrics()
        {
            TestMetricSystemFunctioning(1, 1, true);
        }

        /// <summary>
        /// Tests UnregisterSource functionality of metrics system.
        /// </summary>
        [Fact]
        public void TestMetricsSystemUnreigsterSource()
        {
            const string sourceName = "default source";
            const int counters = 1;
            const int numSources = 2;
            const int waitTime = 2000;

            string[] sourceIds = new string[numSources];
            string[][] counterNames = new string[numSources][];
            DefaultMetricsSourceImpl[] sources = new DefaultMetricsSourceImpl[numSources];
            IMetricsSystem metricSystem = GetMetricsSystemInstance(true);

            for (int i = 0; i < numSources; i++)
            {
                sourceIds[i] = sourceName + "-" + i;
                sources[i] = GetDefaultSource(sourceIds[i]);
                counterNames[i] = GenerateNames(counters, sourceIds[i], "counter");
                metricSystem.RegisterSource(sourceIds[i], sourceIds[i], sources[i]);
                CreateAndUpdateCounters(sources[i], counterNames[i]);
            }

            MetricTestUtils.MetricsSinkForTests sink = new MetricTestUtils.MetricsSinkForTests();
            metricSystem.Subscribe(sink);
            metricSystem.Start();
            metricSystem.UnRegisterSource(sourceIds[0]);
            metricSystem.Stop();

            int initRecordCount = sink.Records.Count;
            ManualResetEvent resetEvent = new ManualResetEvent(false);
            sink.ConditionToCheck = () =>
            {
                if (sink.Records.Count >= initRecordCount + 2)
                {
                    resetEvent.Set();
                }
            };

            metricSystem.Start();
            if (resetEvent.WaitOne(waitTime))
            {
                var records = sink.Records;
                Assert.NotEqual(records[initRecordCount].Name, "recName-" + sourceIds[0]);
                Assert.NotEqual(records[initRecordCount + 1].Name, "recName-" + sourceIds[0]);
            }
            else
            {
                Assert.False(true, "Stopped receiving records from registered source also");   
            }
        }

        /// <summary>
        /// Tests unsubscribing sink in metrics system.
        /// </summary>
        [Fact]
        public void TestMetricsSystemUnSubscribeSink()
        {
            const string sourceName = "default source";
            const int waitTime = 2000;
            const int numSinks = 2;

            IMetricsSystem metricSystem = GetMetricsSystemInstance(true);
            var source = GetDefaultSource(sourceName);
            var counterNames = GenerateNames(1, sourceName, "counter");
            metricSystem.RegisterSource(sourceName, sourceName, source);
            CreateAndUpdateCounters(source, counterNames);

            MetricTestUtils.MetricsSinkForTests[] sinks = new MetricTestUtils.MetricsSinkForTests[numSinks];
            IDisposable[] sinkDisposal = new IDisposable[numSinks];
            
            for (int i = 0; i < sinks.Length; i++)
            {
                sinks[i] = new MetricTestUtils.MetricsSinkForTests();
                sinkDisposal[i] = metricSystem.Subscribe(sinks[i]);
            }

            metricSystem.Start();
            sinkDisposal[0].Dispose();

            int initRecordCount = sinks[0].Records.Count;
            ManualResetEvent resetEvent = new ManualResetEvent(false);
            sinks[0].ConditionToCheck = () =>
            {
                if (sinks[0].Records.Count > initRecordCount)
                {
                    resetEvent.Set();
                }
            };
            if (resetEvent.WaitOne(waitTime))
            {
                Assert.True(false, "No new records can be added to sink since it is un-subscribed");
            }
        }

        /// <summary>
        /// Tests restarting the metrics system.
        /// </summary>
        [Fact]
        public void TestMetricsSystemRestart()
        {
            const string sourceName = "default source";
            const int waitTime = 2000;

            IMetricsSystem metricSystem = GetMetricsSystemInstance(false);
            var source = GetDefaultSource(sourceName);
            var counterNames = GenerateNames(1, sourceName, "counter");
            metricSystem.RegisterSource(sourceName, sourceName, source);
            CreateAndUpdateCounters(source, counterNames);

            MetricTestUtils.MetricsSinkForTests sink = new MetricTestUtils.MetricsSinkForTests();
            metricSystem.Subscribe(sink);
            
            metricSystem.Start();
            metricSystem.Stop();
            int initRecordCount = sink.Records.Count;

            ManualResetEvent resetEvent = new ManualResetEvent(false);
            sink.ConditionToCheck = () =>
            {
                if (sink.Records.Count > initRecordCount)
                {
                    resetEvent.Set();
                }
            };

            metricSystem.Start();
            if (!resetEvent.WaitOne(waitTime))
            {
                Assert.True(false, "No records added to sink after restart");
            }
        }

        /// <summary>
        /// Tests the metrics system shutdown.
        /// </summary>
        [Fact]
        public void TestMetricsSystemShutDown()
        {
            const string sourceName = "default source";
            const int waitTime = 2000;

            IMetricsSystem metricSystem = GetMetricsSystemInstance(false);
            var source = GetDefaultSource(sourceName);
            var counterNames = GenerateNames(1, sourceName, "counter");
            metricSystem.RegisterSource(sourceName, sourceName, source);
            CreateAndUpdateCounters(source, counterNames);

            MetricTestUtils.MetricsSinkForTests sink = new MetricTestUtils.MetricsSinkForTests();
            metricSystem.Subscribe(sink);

            metricSystem.Start();
            metricSystem.Stop();
            metricSystem.ShutDown();

            int initRecordCount = sink.Records.Count;
            ManualResetEvent resetEvent = new ManualResetEvent(false);
            sink.ConditionToCheck = () =>
            {
                if (sink.Records.Count > initRecordCount)
                {
                    resetEvent.Set();
                }
            };
            if (resetEvent.WaitOne(waitTime))
            {
                Assert.False(true, "Metrics system is in shutdown mode. Records cannot be pushed to the sink");
            }
        }

        /// <summary>
        /// Tests if exception is thrown if metrics system is started after 
        /// shutdown.
        /// </summary>
        [Fact]
        public void TestMetricsSystemStartAfterShutDownException()
        {
            const string sourceName = "default source";

            IMetricsSystem metricSystem = GetMetricsSystemInstance(false);
            var source = GetDefaultSource(sourceName);
            metricSystem.RegisterSource(sourceName, sourceName, source);

            MetricTestUtils.MetricsSinkForTests sink = new MetricTestUtils.MetricsSinkForTests();
            metricSystem.Subscribe(sink);

            metricSystem.Start();
            metricSystem.Stop();
            metricSystem.ShutDown();
            Assert.Throws<MetricsException>(() => metricSystem.Start());
        }

        /// <summary>
        /// Tests immediate publishing of metrics. The periodic timer in metrics 
        /// system is set to a very high value.
        /// </summary>
        [Fact]
        public void TestPubishMetricsNow()
        {
            // Set to max value so that timer is never invoked.
            const int timer = int.MaxValue;
            const string sourceName = "default source";
            const int waitTime = 2000;

            IMetricsSystem metricSystem = GetMetricsSystemInstance(true, timer);
            var source = GetDefaultSource(sourceName);
            var counterNames = GenerateNames(1, sourceName, "counter");
            metricSystem.RegisterSource(sourceName, sourceName, source);
            CreateAndUpdateCounters(source, counterNames);

            MetricTestUtils.MetricsSinkForTests sink = new MetricTestUtils.MetricsSinkForTests();
            metricSystem.Subscribe(sink);

            ManualResetEvent resetEvent = new ManualResetEvent(false);
            sink.ConditionToCheck = () =>
            {
                if (sink.Records.Count == 2)
                {
                    resetEvent.Set();
                }
            };
            metricSystem.Start();

            metricSystem.PublishMetricsNow(true);
            metricSystem.PublishMetricsNow(true);

            if (!resetEvent.WaitOne(waitTime))
            {
                Assert.False(true, "Did not receive expected number of records.");
            }
        }

        /// <summary>
        /// Tests <see cref="MetricsSystemContext"/>.
        /// </summary>
        [Fact]
        public void TestMetricsSystemOnContextStart()
        {
            const int timer = 100;
            const int waitTime = 2000;

            var contextConfig = Configurations.Merge(
                MetricsContextConfiguration.ConfigurationModule
                    .Set(MetricsContextConfiguration.Sink, GenericType<MetricTestUtils.MetricsSinkForTests>.Class)
                    .Build(),
                GetMetricsSystemConfig(true, timer));
            var injector = TangFactory.GetTang().NewInjector(contextConfig);
            
            var metricsSystemContext = injector.GetInstance<MetricsSystemContext>();
            var sink = injector.GetNamedInstance<ContextSinkParameters.SinkSetName, ISet<IObserver<IMetricsRecord>>>(
                GenericType<ContextSinkParameters.SinkSetName>.Class).First() as MetricTestUtils.MetricsSinkForTests;
            if (sink == null)
            {
                throw new NullReferenceException("Sink cannot be null");
            }

            ManualResetEvent resetEvent = new ManualResetEvent(false);
            sink.ConditionToCheck = () =>
            {
                if (sink.Records.Count > 0)
                {
                    resetEvent.Set();
                }
            };

            metricsSystemContext.OnNext(new DummyStartContext());

            var source = injector.GetInstance<DefaultMetricsSourceImpl>();
            var counterNames = GenerateNames(1, "default-name", "counter");
            CreateAndUpdateCounters(source, counterNames);
            
            if (!resetEvent.WaitOne(waitTime))
            {
                Assert.True(false, "Sink should have received some records.");
            }
        }

        private class DummyStartContext : IContextStart
        {
            public DummyStartContext()
            {
                Id = "Id";
            }

            public string Id { get; set; }
        }

        /// <summary>
        /// Tests functioning of metrics system under different 
        /// sink and source settings.
        /// </summary>
        /// <param name="numSources">Number of sources.</param>
        /// <param name="numSinks">Number of sinks.</param>
        /// <param name="getUnchangedMetrics">Whether to gt even unchanged metrics.</param>
        private static void TestMetricSystemFunctioning(int numSources, int numSinks, bool getUnchangedMetrics)
        {
            const string sourceName = "default source";
            const int counters = 1;
            const int longGauges = 3;
            const int doubleGauges = 2;
            const int tags = 2;
            const int waitTime = 2000;

            string[] sourceIds = new string[numSources];
            string[][] counterNames = new string[numSources][];
            string[][] longGaugeNames = new string[numSources][];
            string[][] doubleGaugeNames = new string[numSources][];
            string[][] tagNames = new string[numSources][];
            string[][] tagValues = new string[numSources][];
            DefaultMetricsSourceImpl[] sources = new DefaultMetricsSourceImpl[numSources];
            IMetricsSystem metricSystem = GetMetricsSystemInstance(getUnchangedMetrics);

            for (int i = 0; i < numSources; i++)
            {
                sourceIds[i] = sourceName + "-" + i;
                sources[i] = GetDefaultSource(sourceIds[i]);
                counterNames[i] = GenerateNames(counters, sourceIds[i], "counter");
                longGaugeNames[i] = GenerateNames(longGauges, sourceIds[i], "longGauge");
                doubleGaugeNames[i] = GenerateNames(doubleGauges, sourceIds[i], "doubleGauge");
                tagNames[i] = GenerateNames(tags, sourceIds[i], "tag");
                tagValues[i] = tagNames[i].Select(x => x + "-Value").ToArray();
                metricSystem.RegisterSource(sourceIds[i], sourceIds[i], sources[i]);
                CreateAndUpdateCounters(sources[i], counterNames[i]);
                CreateAndUpdateDoubleGauges(sources[i], doubleGaugeNames[i]);
                CreateAndUpdateLongGauge(sources[i], longGaugeNames[i]);
                CreateAndUpdateTags(sources[i], tagNames[i], tagValues[i]);
            }

            MetricTestUtils.MetricsSinkForTests[] sinks = new MetricTestUtils.MetricsSinkForTests[numSinks];
            for (int i = 0; i < sinks.Length; i++)
            {
                sinks[i] = new MetricTestUtils.MetricsSinkForTests();
                metricSystem.Subscribe(sinks[i]);
            }

            ManualResetEvent[] resetEvent = new ManualResetEvent[numSinks];

            for (int i = 0; i < sinks.Length; i++)
            {
                resetEvent[i] = new ManualResetEvent(false);
                var i1 = i;
                sinks[i].ConditionToCheck = () =>
                {
                    if (sinks[i1].Records.Count >= 2 * numSources)
                    {
                        resetEvent[i1].Set();
                    }
                };
            }

            metricSystem.Start();

            for (int k = 0; k < sinks.Length; k++)
            {
                var sink = sinks[k];
                if (!resetEvent[k].WaitOne(waitTime))
                {
                    Assert.False(true, "All the metrics from source not received at sink");
                }
                else
                {
                    var records = sink.Records as List<IMetricsRecord>;
                    int recordsWithMetrics = getUnchangedMetrics ? 2 * numSources : numSources;

                    for (int m = 0; m < recordsWithMetrics; m++)
                    {
                        if (records != null)
                        {
                            var record = records[m];
                            int srcIndex = m % numSources;
                            AssertMetricRecordEquality(record,
                                counterNames[srcIndex],
                                counterNames[srcIndex].Select(x => (long)1).ToArray(),
                                longGaugeNames[srcIndex],
                                longGaugeNames[srcIndex].Select(x => (long)2).ToArray(),
                                doubleGaugeNames[srcIndex],
                                doubleGaugeNames[srcIndex].Select(x => 0.2).ToArray(),
                                tagNames[srcIndex],
                                tagValues[srcIndex]);
                        }
                        else
                        {
                            Assert.False(true, "Records cannot be null in sinks.");
                        }
                    }

                    for (int m = recordsWithMetrics; m < 2 * numSources; m++)
                    {
                        if (records != null)
                        {
                            Assert.True(!records[m].Metrics.Any(),
                                "Record should not haveany metric since their values were unchanged");
                        }
                    }
                }
            }
            metricSystem.Stop();
            metricSystem.ShutDown();
        }

        private static void CreateAndUpdateCounters(DefaultMetricsSourceImpl source, string[] counterNames)
        {
            foreach (string name in counterNames)
            {
                source.Counters.Create(name, name);
                source.Counters[name].Increment();
            }
        }

        private static void CreateAndUpdateLongGauge(DefaultMetricsSourceImpl source, string[] longGaugeNames)
        {
            foreach (string name in longGaugeNames)
            {
                source.LongGauges.Create(name, name);
                source.LongGauges[name].Increment(2);
            }
        }

        private static void CreateAndUpdateDoubleGauges(DefaultMetricsSourceImpl source, string[] doubleGaugeNames)
        {
            foreach (string name in doubleGaugeNames)
            {
                source.DoubleGauges.Create(name, name);
                source.DoubleGauges[name].Increment(0.2);
            }
        }

        private static void CreateAndUpdateTags(DefaultMetricsSourceImpl source, string[] tagNames, string[] tagValues)
        {
            for (int counter = 0; counter < tagNames.Length; counter++)
            {
                source.AddTag(tagNames[counter], tagNames[counter], tagValues[counter]);
            }
        }

        private static IMetricsSystem GetMetricsSystemInstance(bool getUnchangedMetrics, int timer = 100)
        {
            return
                TangFactory.GetTang().NewInjector(GetMetricsSystemConfig(getUnchangedMetrics, timer))
                    .GetInstance<IMetricsSystem>();
        }

        private static IConfiguration GetMetricsSystemConfig(bool getUnchangedMetrics, int timer)
        {
            const int sinkRetryCount = 2;
            const int queueCapacity = 1000;
            const int minRetry = 500;
            const int maxRetry = 1000;
            const int deltaBackOff = 200;

            return
                MetricsSystemConfiguration.ConfigurationModule
                    .Set(MetricsSystemConfiguration.PeriodicTimer, timer.ToString())
                    .Set(MetricsSystemConfiguration.DeltaBackOffInMs, deltaBackOff.ToString())
                    .Set(MetricsSystemConfiguration.SinkMinRetryIntervalInMs, minRetry.ToString())
                    .Set(MetricsSystemConfiguration.SinkMaxRetryIntervalInMs, maxRetry.ToString())
                    .Set(MetricsSystemConfiguration.SinkQueueCapacity, queueCapacity.ToString())
                    .Set(MetricsSystemConfiguration.SinkRetries, sinkRetryCount.ToString())
                    .Set(MetricsSystemConfiguration.GetUnchangedMetrics, getUnchangedMetrics.ToString())
                    .Build();
        }

        private static DefaultMetricsSourceImpl GetDefaultSource(string sourceId)
        {
            string sourceName = sourceId;
            string evalId = "eval Id-" + sourceId;
            string recordName = "recName-" + sourceId;
            string sourceContext = "test-" + sourceId;

            return
                TangFactory.GetTang().NewInjector(DefaultMetricsSourceConfiguration.ConfigurationModule
                    .Set(DefaultMetricsSourceConfiguration.EvaluatorId, evalId)
                    .Set(DefaultMetricsSourceConfiguration.TaskOrContextId, sourceName)
                    .Set(DefaultMetricsSourceConfiguration.RecordId, recordName)
                    .Set(DefaultMetricsSourceConfiguration.SourceContext, sourceContext)
                    .Build())
                    .GetInstance<DefaultMetricsSourceImpl>();
        }

        private static void AssertMetricRecordEquality(IMetricsRecord record,
            string[] counterNames,
            long[] counterValues,
            string[] longGaugeNames,
            long[] longGaugeValues,
            string[] doubleGaugeNames,
            double[] doubleGaugeValues,
            string[] tagNames,
            string[] tagValues)
        {
            int metricCounter = counterNames.Length + longGaugeNames.Length + doubleGaugeNames.Length;
            Assert.Equal(metricCounter, record.Metrics.Count());
            Assert.True(record.Tags.Count() > tagNames.Length);

            List<IImmutableMetric> metrics = record.Metrics as List<IImmutableMetric>;
            List<MetricsTag> tags = record.Tags as List<MetricsTag>;
            List<string> metricNames = record.Metrics.Select(x => x.Info.Name).ToList();
            List<string> recordTagNames = record.Tags.Select(x => x.Info.Name).ToList();

            if (metrics == null)
            {
                Assert.False(true, "Metrics in metric record cannot be null");
            }

            for (int i = 0; i < counterNames.Length; i++)
            {
                AssertMetricEquality(metrics, metricNames, counterNames[i], counterValues[i], MetricType.Counter);
            }
            for (int i = 0; i < longGaugeNames.Length; i++)
            {
                AssertMetricEquality(metrics, metricNames, longGaugeNames[i], longGaugeValues[i], MetricType.Gauge);
            }
            for (int i = 0; i < counterNames.Length; i++)
            {
                AssertMetricEquality(metrics, metricNames, doubleGaugeNames[i], doubleGaugeValues[i]);
            }
            for (int i = 0; i < tagNames.Length; i++)
            {
                AssertTagEquality(tags, recordTagNames, tagNames[i], tagValues[i]);
            }
        }

        private static void AssertTagEquality(List<MetricsTag> tags,
          List<string> tagNames,
          string name,
          string tagValue)
        {
            int index = tagNames.IndexOf(name);
            if (index == -1)
            {
                Assert.False(true, string.Format("Metric with name {0} does not exist in record.", name));
            }
            Assert.Equal(tags[index].Value, tagValue);
        }

        private static void AssertMetricEquality(List<IImmutableMetric> metrics,
            List<string> metricNames,
            string name,
            long lValue,
            MetricType metricType)
        {
            int index = metricNames.IndexOf(name);
            if (index == -1)
            {
                Assert.False(true, string.Format("Metric with name {0} does not exist in record.", name));
            }
            Assert.Equal(metrics[index].LongValue, lValue);
            Assert.Equal(metrics[index].TypeOfMetric, metricType);
        }

        private static void AssertMetricEquality(List<IImmutableMetric> metrics,
           List<string> metricNames,
           string name,
           double dValue)
        {
            int index = metricNames.IndexOf(name);
            if (index == -1)
            {
                Assert.False(true, string.Format("Metric with name {0} does not exist in record.", name));
            }
            Assert.Equal(metrics[index].NumericValue, dValue);
            Assert.Equal(metrics[index].TypeOfMetric, MetricType.Gauge);
        }

        private static string[] GenerateNames(int numNames, string sourceName, string metricType)
        {
            string[] metricNames = new string[numNames];
            for (int i = 0; i < numNames; i++)
            {
                metricNames[i] = sourceName + "-" + metricType + "-" + i;
            }
            return metricNames;
        }
    }
}
