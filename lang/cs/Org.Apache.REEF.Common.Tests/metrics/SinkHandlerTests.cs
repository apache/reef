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
using Xunit;
using Org.Apache.REEF.Common.Metrics.MetricsSystem;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Common.Metrics.MetricsSystem.Parameters;

namespace Org.Apache.REEF.Common.Tests.Metrics
{
    /// <summary>
    /// Tests various <see cref="SinkHandler"/> functions.
    /// </summary>
    public class SinkHandlerTests
    {
        /// <summary>
        /// Tests normal functioning of Sink Handler.
        /// </summary>
        [Fact]
        public void TestSinkHandlerNormalFunctioning()
        {
            const int retryCount = 2;
            const int queueCapacity = 1000;
            const int minRetry = 500;
            const int maxRetry = 1000;
            const int deltaBackOff = 200;
            const int counters = 1;
            const int longGauges = 0;
            const int doubleGauges = 2;
            const int tags = 2;
            const int recsPerSnapshot = 2;
            const int snapshotsPerRequest = 2;
            const int numSnapshotRequests = 2;

            var sink = new MetricTestUtils.MetricsSinkForTests();
            var sinkParams =
                GetSinkHandlerParameter(retryCount,
                    queueCapacity,
                    minRetry,
                    maxRetry,
                    deltaBackOff);

            SinkHandler sinkHandler = new SinkHandler(sinkParams, sink);
            sinkHandler.Start();

            var snapshotRequests = GenerateSnapshotRequests(numSnapshotRequests,
                snapshotsPerRequest,
                recsPerSnapshot,
                counters,
                doubleGauges,
                longGauges,
                tags);

            foreach (var request in snapshotRequests)
            {
                sinkHandler.PutMetricsInQueue(request);
            }

            sinkHandler.Stop();

            var actualRecords = sink.Records.ToArray();
            var expectedRecords = snapshotRequests.SelectMany(x => x.SelectMany(y => y.Records)).ToArray();
            AssertArrayOfRecordsEquality(expectedRecords, actualRecords);
        }

        /// <summary>
        /// Tests SinkHandler restart functioning.
        /// </summary>
        [Fact]
        public void TestSinkHandlerRestartFunctioning()
        {
            const int retryCount = 2;
            const int queueCapacity = 1000;
            const int minRetry = 500;
            const int maxRetry = 1000;
            const int deltaBackOff = 200;
            const int counters = 1;
            const int longGauges = 0;
            const int doubleGauges = 2;
            const int tags = 2;
            const int recsPerSnapshot = 2;
            const int snapshotsPerRequest = 2;
            const int numSnapshotRequests = 3;

            var sink = new MetricTestUtils.MetricsSinkForTests();
            var sinkParams =
                GetSinkHandlerParameter(retryCount,
                    queueCapacity,
                    minRetry,
                    maxRetry,
                    deltaBackOff);

            SinkHandler sinkHandler = new SinkHandler(sinkParams, sink);

            var snapshotRequests = GenerateSnapshotRequests(numSnapshotRequests,
                snapshotsPerRequest,
                recsPerSnapshot,
                counters,
                doubleGauges,
                longGauges,
                tags);

            int counter = 0;
            foreach (var request in snapshotRequests)
            {
                counter++;
                sinkHandler.Start();
                sinkHandler.PutMetricsInQueue(request);
                sinkHandler.Stop();
                var actualRecords = sink.Records.ToArray();
                var expectedRecords =
                    snapshotRequests.Take(counter).SelectMany(x => x.SelectMany(y => y.Records)).ToArray();
                AssertArrayOfRecordsEquality(expectedRecords, actualRecords);
            }
        }

        /// <summary>
        /// Tests whether exception is thown if we try to restart already 
        /// started Sink handler.
        /// </summary>
        [Fact]
        public void TestSinkHandlerRestartException()
        {
            const int retryCount = 2;
            const int queueCapacity = 1000;
            const int minRetry = 500;
            const int maxRetry = 1000;
            const int deltaBackOff = 200;

            var sink = new MetricTestUtils.MetricsSinkForTests();
            var sinkParams =
                GetSinkHandlerParameter(retryCount,
                    queueCapacity,
                    minRetry,
                    maxRetry,
                    deltaBackOff);

            SinkHandler sinkHandler = new SinkHandler(sinkParams, sink);
            sinkHandler.Start();
            Assert.Throws<MetricsException>(() => sinkHandler.Start());
            Assert.True(sink.Error);
        }

        /// <summary>
        /// Tests whether exception is thown if we try to stop already 
        /// stopped Sink handler.
        /// </summary>
        [Fact]
        public void TestSinkHandlerRestopException()
        {
            const int retryCount = 2;
            const int queueCapacity = 1000;
            const int minRetry = 500;
            const int maxRetry = 1000;
            const int deltaBackOff = 200;

            var sink = new MetricTestUtils.MetricsSinkForTests();
            var sinkParams =
                GetSinkHandlerParameter(retryCount,
                    queueCapacity,
                    minRetry,
                    maxRetry,
                    deltaBackOff);

            SinkHandler sinkHandler = new SinkHandler(sinkParams, sink);
            sinkHandler.Start();
            sinkHandler.Stop();
            Assert.Throws<MetricsException>(() => sinkHandler.Stop());
            Assert.True(sink.Error);
        }

        /// <summary>
        /// Tests shut down function of Sink Handler.
        /// </summary>
        [Fact]
        public void TestSinkHandlerShutDown()
        {
            const int retryCount = 2;
            const int queueCapacity = 1000;
            const int minRetry = 500;
            const int maxRetry = 1000;
            const int deltaBackOff = 200;

            var sink = new MetricTestUtils.MetricsSinkForTests();
            var sinkParams =
                GetSinkHandlerParameter(retryCount,
                    queueCapacity,
                    minRetry,
                    maxRetry,
                    deltaBackOff);

            SinkHandler sinkHandler = new SinkHandler(sinkParams, sink);
            sinkHandler.Start();
            sinkHandler.Stop();
            sinkHandler.ShutDown();
            Assert.True(sink.ShutDown);
        }

        /// <summary>
        /// Tests if exception is thrown if start is called after shutdown.
        /// </summary>
        [Fact]
        public void TestSinkHandlerStartExceptionAfterShutDown()
        {
            const int retryCount = 2;
            const int queueCapacity = 1000;
            const int minRetry = 500;
            const int maxRetry = 1000;
            const int deltaBackOff = 200;

            var sink = new MetricTestUtils.MetricsSinkForTests();
            var sinkParams =
                GetSinkHandlerParameter(retryCount,
                    queueCapacity,
                    minRetry,
                    maxRetry,
                    deltaBackOff);

            SinkHandler sinkHandler = new SinkHandler(sinkParams, sink);
            sinkHandler.Start();
            sinkHandler.Stop();
            sinkHandler.ShutDown();
            Assert.Throws<MetricsException>(() => sinkHandler.Start());
        }

        private static List<List<SourceSnapshot>> GenerateSnapshotRequests(int numSnapshotRequests,
            int snapshotsPerRequest,
            int recsPerSnapshot,
            int counters,
            int doubleGauges,
            int longGauges,
            int tags)
        {
            List<List<SourceSnapshot>> snapshotRequests = new List<List<SourceSnapshot>>();
            for (int m = 0; m < numSnapshotRequests; m++)
            {
                List<SourceSnapshot> snapshotRequest = new List<SourceSnapshot>();
                for (int k = 0; k < snapshotsPerRequest; k++)
                {
                    string sourceName = "Source-" + k;
                    IMetricsRecord[] records = new IMetricsRecord[recsPerSnapshot];
                    for (int i = 0; i < records.Length; i++)
                    {
                        string recName = "Snapshot-" + k + "-Rec-" + i;
                        records[i] = GenerateMetricRecord(recName, counters, longGauges, doubleGauges, tags);
                    }
                    snapshotRequest.Add(new SourceSnapshot(sourceName, records));
                }
                snapshotRequests.Add(snapshotRequest);
            }
            return snapshotRequests;
        }

        private static IMetricsRecord GenerateMetricRecord(string recordName,
            int counters,
            int longGauges,
            int doubleGauges,
            int tags)
        {
            Random randGen = new Random();
            const long timeStamp = 2000;
            const string context = "TestContext";
            List<IImmutableMetric> metrics = new List<IImmutableMetric>();
            List<MetricsTag> tagList = new List<MetricsTag>();

            for (int i = 0; i < counters; i++)
            {
                string counterName = "Counter" + i;
                metrics.Add(new ImmutableCounter(new MetricsInfoImpl(counterName, counterName), randGen.Next()));
            }

            for (int i = 0; i < longGauges; i++)
            {
                string gaugeName = "LGauge" + i;
                metrics.Add(new ImmutableLongGauge(new MetricsInfoImpl(gaugeName, gaugeName), randGen.Next()));
            }

            for (int i = 0; i < doubleGauges; i++)
            {
                string gaugeName = "DGauge" + i;
                metrics.Add(new ImmutableDoubleGauge(new MetricsInfoImpl(gaugeName, gaugeName), randGen.NextDouble()));
            }

            for (int i = 0; i < tags; i++)
            {
                string tagVal = "tagVal" + randGen.Next();
                string tagName = "tagName" + randGen.Next();
                tagList.Add(new MetricsTag(new MetricsInfoImpl(tagName, tagName), tagVal));
            }

            return new MetricsRecord(new MetricsInfoImpl(recordName, recordName), timeStamp, metrics, tagList, context);
        }

        private static SinkHandlerParameters GetSinkHandlerParameter(int retryCount,
            int queueCapacity,
            int minRetry,
            int maxRetry,
            int deltaBackOff)
        {
            return TangFactory.GetTang()
                .NewInjector(TangFactory.GetTang().NewConfigurationBuilder()
                    .BindNamedParameter(typeof(SinkRetryCount), retryCount.ToString())
                    .BindNamedParameter(typeof(SinkDeltaBackOffInMs), deltaBackOff.ToString())
                    .BindNamedParameter(typeof(SinkMaxRetryIntervalInMs), maxRetry.ToString())
                    .BindNamedParameter(typeof(SinkMinRetryIntervalInMs), minRetry.ToString())
                    .BindNamedParameter(typeof(SinkQueueCapacity), queueCapacity.ToString())
                    .Build())
                .GetInstance<SinkHandlerParameters>();
        }

        private static void AssertArrayOfRecordsEquality(IMetricsRecord[] expectedRecords,
         IMetricsRecord[] actualRecords)
        {
            Assert.Equal(expectedRecords.Length, actualRecords.Length);

            for (int i = 0; i < expectedRecords.Length; i++)
            {
                AssertRecordEquality(expectedRecords[i], actualRecords[i]);
            }
        }

        private static void AssertMetricEquality(IImmutableMetric metric1, IImmutableMetric metric2)
        {
            Assert.Equal(metric1.Info.Name, metric2.Info.Name);
            Assert.Equal(metric1.Info.Description, metric2.Info.Description);
            Assert.Equal(metric1.TypeOfMetric, metric2.TypeOfMetric);

            if (metric1.NumericValue != null)
            {
                Assert.Equal(metric1.NumericValue, metric2.NumericValue);
            }
            else
            {
                Assert.Equal(metric1.LongValue, metric2.LongValue);
            }
        }

        private static void AssertTagEquality(MetricsTag tag1, MetricsTag tag2)
        {
            Assert.Equal(tag1.Name, tag2.Name);
            Assert.Equal(tag1.Description, tag2.Description);
            Assert.Equal(tag1.Value, tag2.Value);
        }

        private static void AssertRecordEquality(IMetricsRecord record1, IMetricsRecord record2)
        {
            Assert.Equal(record1.Name, record2.Name);
            Assert.Equal(record1.Description, record2.Description);
            Assert.Equal(record1.Timestamp, record2.Timestamp);

            if (record1.Context != null || record2.Context != null)
            {
                Assert.Equal(record1.Context, record2.Context);
            }

            var metrics1 = record1.Metrics.ToArray();
            var metrics2 = record2.Metrics.ToArray();
            Assert.Equal(metrics1.Length, metrics2.Length);
            for (int i = 0; i < metrics1.Length; i++)
            {
                AssertMetricEquality(metrics1[i], metrics2[i]);
            }

            var tags1 = record1.Tags.ToArray();
            var tags2 = record2.Tags.ToArray();
            Assert.Equal(tags1.Length, tags2.Length);
            for (int i = 0; i < tags1.Length; i++)
            {
                AssertTagEquality(tags1[i], tags2[i]);
            }
        }
    }
}
