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

using Xunit;
using Org.Apache.REEF.Common.Metrics.MetricsSystem;
using Org.Apache.REEF.Common.Metrics.MutableMetricsLayer;
using Org.Apache.REEF.Tang.Implementations.Tang;

namespace Org.Apache.REEF.Common.Tests.Metrics
{
    /// <summary>
    /// Tests source handler.
    /// </summary>
    public class SourceHandlerTests
    {
        /// <summary>
        /// Tests functioning of source handler. A <see cref="DefaultMetricsSourceImpl"/> instance 
        /// is passed to the <see cref="MetricsSourceHandler"/>. It is then verified that 
        /// the metrics created and updated in the source are passed on correctly to the 
        /// collector once GetMetrics is called. Further, both variants of the function call 
        /// (get changed metrics only and get even unchanged metrics) are tested.
        /// </summary>
        [Fact]
        public void TestSourceHandler()
        {
            const string sourceName = "sourceName";
            const string evalId = "evalId";
            const string taskId = "taskId";
            const string sourceContext = "sContext";
            const string recordName = "recName";
            const string counterName = "counter";

            DefaultMetricsSourceImpl source =
                TangFactory.GetTang().NewInjector(DefaultMetricSourceTests.GenerateMetricsSourceConfiguration(evalId,
                    taskId,
                    sourceContext,
                    recordName)).GetInstance<DefaultMetricsSourceImpl>();

            MetricsSourceHandler sourceHandler = new MetricsSourceHandler(sourceName, source);

            Assert.Equal(sourceName, sourceHandler.Name);
            Assert.Equal(source, sourceHandler.Source);

            source.Counters.Create(counterName, counterName);
            source.Counters[counterName].Increment();

            MetricTestUtils.MetricsCollectorTestImpl collector = new MetricTestUtils.MetricsCollectorTestImpl();
            source.GetMetrics(collector, true);
            collector.CurrentRecordBuilder.Validate(counterName, 1);

            source.GetMetrics(collector, false);
            Assert.False(collector.CurrentRecordBuilder.LongKeyPresent(counterName));
        }
    }
}