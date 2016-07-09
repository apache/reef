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
using Xunit;

namespace Org.Apache.REEF.Common.Tests.Metrics
{
    /// <summary>
    /// Contains functions to test <see cref="SnapshotRequest"/> clkass.
    /// </summary>
    public sealed class SnapshotRequestTest
    {
        /// <summary>
        /// Tests <see cref="SnapshotRequest"/>.
        /// </summary>
        [Fact]
        public void TestSnapshotRequest()
        {
            var request = new SnapshotRequest(new MetricsRecordBuilderTestImpl(), true);
            Assert.NotNull(request.Builder);
            Assert.Equal(request.FullSnapshot, true);

            request = new SnapshotRequest(new MetricsRecordBuilderTestImpl(), false);
            Assert.NotNull(request.Builder);
            Assert.Equal(request.FullSnapshot, false);

            request = new SnapshotRequest(new MetricsRecordBuilderTestImpl());
            Assert.NotNull(request.Builder);
            Assert.Equal(request.FullSnapshot, false);
        }

        private class MetricsRecordBuilderTestImpl : IMetricsRecordBuilder
        {
            public IMetricsRecordBuilder AddTag(string name, string value)
            {
                throw new System.NotImplementedException();
            }

            public IMetricsRecordBuilder AddTag(IMetricsInfo info, string value)
            {
                throw new System.NotImplementedException();
            }

            public IMetricsRecordBuilder Add(MetricsTag tag)
            {
                throw new System.NotImplementedException();
            }

            public IMetricsRecordBuilder Add(IImmutableMetric metric)
            {
                throw new System.NotImplementedException();
            }

            public IMetricsRecordBuilder SetContext(string value)
            {
                throw new System.NotImplementedException();
            }

            public IMetricsRecordBuilder AddCounter(IMetricsInfo info, long value)
            {
                throw new System.NotImplementedException();
            }

            public IMetricsRecordBuilder AddGauge(IMetricsInfo info, long value)
            {
                throw new System.NotImplementedException();
            }

            public IMetricsRecordBuilder AddGauge(IMetricsInfo info, double value)
            {
                throw new System.NotImplementedException();
            }

            public IMetricsCollector ParentCollector()
            {
                throw new System.NotImplementedException();
            }

            public IMetricsCollector EndRecord()
            {
                throw new System.NotImplementedException();
            }
        }
    }
}
