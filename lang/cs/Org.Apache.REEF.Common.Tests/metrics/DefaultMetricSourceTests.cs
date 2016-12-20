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

using System.Linq;
using Org.Apache.REEF.Common.Metrics.Api;
using Org.Apache.REEF.Common.Metrics.MutableMetricsLayer;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Xunit;

namespace Org.Apache.REEF.Common.Tests.Metrics
{
    /// <summary>
    ///  Tests different functionalities of <see cref="DefaultMetricsSourceImpl"/>
    /// </summary>
    public sealed class DefaultMetricSourceTests
    {
        /// <summary>
        /// Tests <see cref="DefaultMetricsSourceConfiguration"/>
        /// </summary>
        [Fact]
        public void TestMetricsSourceConfiguration()
        {
            const string evalId = "evalId";
            const string taskId = "taskId";
            const string sourceContext = "test";
            const string recordName = "testrecord";

            DefaultMetricsSourceImpl metricsSource = TangFactory.GetTang().NewInjector(
                GenerateMetricsSourceConfiguration(evalId, taskId, sourceContext, recordName))
                .GetInstance<DefaultMetricsSourceImpl>();

            Assert.NotNull(metricsSource);

            MetricTestUtils.MetricsCollectorTestImpl collector = new MetricTestUtils.MetricsCollectorTestImpl();
            metricsSource.GetMetrics(collector, true);
            Assert.Equal(collector.CurrentRecordBuilder.Name, recordName);
            Assert.Equal(collector.CurrentRecordBuilder.Context, sourceContext);
            collector.CurrentRecordBuilder.Validate("TaskOrContextName", taskId);
            collector.CurrentRecordBuilder.Validate("EvaluatorId", evalId);
        }

        /// <summary>
        /// Tests creating, accessing and updating counters from  <see cref="DefaultMetricsSourceImpl"/>
        /// </summary>
        [Fact]
        public void TestDefaultMetricsSourceCounters()
        {
            const string evalId = "evalId";
            const string taskId = "taskId";
            const string sourceContext = "test";
            const string recordName = "testrecord";
            string[] counterNames = { "cname1", "cname2" };
            const long counterIncr = 2;

            DefaultMetricsSourceImpl metricsSource = TangFactory.GetTang().NewInjector(
                GenerateMetricsSourceConfiguration(evalId, taskId, sourceContext, recordName))
                .GetInstance<DefaultMetricsSourceImpl>();

            MetricTestUtils.MetricsCollectorTestImpl collector = new MetricTestUtils.MetricsCollectorTestImpl();

            // Create Counters
            foreach (var name in counterNames)
            {
                metricsSource.Counters.Create(name, name);
            }
   
            // Update counters.
            int counter = 1;
            foreach (var name in counterNames)
            {
                metricsSource.Counters[name].Increment(counter * counterIncr);
                counter++;
            }

            metricsSource.GetMetrics(collector, true);
            var rb = collector.CurrentRecordBuilder;

            // Validate counters
            counter = 1;
            foreach (var name in counterNames)
            {
                rb.Validate(name, counter * counterIncr);
                counter++;
            }           
        }

        /// <summary>
        /// Tests creating, accessing and updating long gauge from <see cref="DefaultMetricsSourceImpl"/>
        /// </summary>
        [Fact]
        public void TestDefaultMetricsSourceLongGauges()
        {
            const string evalId = "evalId";
            const string taskId = "taskId";
            const string sourceContext = "test";
            const string recordName = "testrecord";
            string[] longGaugeNames = { "lname1", "lname2" };
            const long longGaugeIncr = 3;

            DefaultMetricsSourceImpl metricsSource = TangFactory.GetTang().NewInjector(
                GenerateMetricsSourceConfiguration(evalId, taskId, sourceContext, recordName))
                .GetInstance<DefaultMetricsSourceImpl>();

            MetricTestUtils.MetricsCollectorTestImpl collector = new MetricTestUtils.MetricsCollectorTestImpl();

            // Create long gauge
            foreach (var name in longGaugeNames)
            {
                metricsSource.LongGauges.Create(name, name);
            }

            // Update long gauge
            int counter = 1;
            foreach (var name in longGaugeNames)
            {
                metricsSource.LongGauges[name].Increment(counter * longGaugeIncr);
                counter++;
            }

            metricsSource.GetMetrics(collector, true);
            var rb = collector.CurrentRecordBuilder;

            // Validate long gauges
            counter = 1;
            foreach (var name in longGaugeNames)
            {
                rb.Validate(name, counter * longGaugeIncr);
                counter++;
            }
        }

        /// <summary>
        /// Tests creating, accessing and updating double gauge from <see cref="DefaultMetricsSourceImpl"/>
        /// </summary>
        [Fact]
        public void TestDefaultMetricsSourceDoubleGauges()
        {
            const string evalId = "evalId";
            const string taskId = "taskId";
            const string sourceContext = "test";
            const string recordName = "testrecord";
            string[] doubleGaugeNames = { "dname1", "dname2" };
            const double doubleGaugeDecr = 1.2;

            DefaultMetricsSourceImpl metricsSource = TangFactory.GetTang().NewInjector(
                GenerateMetricsSourceConfiguration(evalId, taskId, sourceContext, recordName))
                .GetInstance<DefaultMetricsSourceImpl>();

            MetricTestUtils.MetricsCollectorTestImpl collector = new MetricTestUtils.MetricsCollectorTestImpl();

            // Create double gauge
            foreach (var name in doubleGaugeNames)
            {
                metricsSource.DoubleGauges.Create(name, name);
            }

            // Update double gauge
            int counter = 1;
            foreach (var name in doubleGaugeNames)
            {
                metricsSource.DoubleGauges[name].Decrement(counter * doubleGaugeDecr);
                counter++;
            }
        
            metricsSource.GetMetrics(collector, true);
            var rb = collector.CurrentRecordBuilder;

            // Validate double gauges
            counter = 1;
            foreach (var name in doubleGaugeNames)
            {
                rb.Validate(name, -counter * doubleGaugeDecr, 1e-10);
                counter++;
            }
        }

        /// <summary>
        /// Tests creating, accessing and updating rate from <see cref="DefaultMetricsSourceImpl"/>
        /// </summary>
        [Fact]
        public void TestDefaultMetricsSourceRate()
        {
            const string evalId = "evalId";
            const string taskId = "taskId";
            const string sourceContext = "test";
            const string recordName = "testrecord";
            const string rateName = "rname1";
            double[] samples = { 2, 3 };

            DefaultMetricsSourceImpl metricsSource = TangFactory.GetTang().NewInjector(
                GenerateMetricsSourceConfiguration(evalId, taskId, sourceContext, recordName))
                .GetInstance<DefaultMetricsSourceImpl>();

            MetricTestUtils.MetricsCollectorTestImpl collector = new MetricTestUtils.MetricsCollectorTestImpl();

            // Create rate.
            metricsSource.Rates.Create(rateName, rateName);

            // Update rate
            foreach (var sample in samples)
            {
                metricsSource.Rates[rateName].Sample(sample);
            }

            metricsSource.GetMetrics(collector, true);
            var rb = collector.CurrentRecordBuilder;
          
            // Validate rate
            rb.Validate(rateName + "-Num", samples.Length);
            rb.Validate(rateName + "-RunningAvg", samples.Sum() / samples.Length, 1e-10);
        }

        /// <summary>
        /// Tests creating and accessing tags from <see cref="DefaultMetricsSourceImpl"/>
        /// </summary>
        [Fact]
        public void TestDefaultMetricsSourceTags()
        {
            const string evalId = "evalId";
            const string taskId = "taskId";
            const string sourceContext = "test";
            const string recordName = "testrecord";
            string[] tagNames = { "tname1", "tname2" };
            string[] tagValues = { "tvalue1", "tValue2" };

            DefaultMetricsSourceImpl metricsSource = TangFactory.GetTang().NewInjector(
                GenerateMetricsSourceConfiguration(evalId, taskId, sourceContext, recordName))
                .GetInstance<DefaultMetricsSourceImpl>();

            MetricTestUtils.MetricsCollectorTestImpl collector = new MetricTestUtils.MetricsCollectorTestImpl();

            // Create tags
            for (int i = 0; i < tagNames.Length; i++)
            {
                metricsSource.AddTag(tagNames[i], tagNames[i], tagValues[i]);
            }

            metricsSource.GetMetrics(collector, true);
            var rb = collector.CurrentRecordBuilder;

            // Validate tags
            for (int i = 0; i < tagNames.Length; i++)
            {
                rb.Validate(tagNames[i], tagValues[i]);
            }

            // Verify GetTag functionality
            var tag = metricsSource.GetTag(tagNames[0]);
            Assert.NotNull(tag);
            Assert.Equal(tag.Name, tagNames[0]);
            Assert.Equal(tag.Value, tagValues[0]);

            string inValidName = "invalid";
            tag = metricsSource.GetTag(inValidName);
            Assert.Null(tag);
        }

        /// <summary>
        /// Tests whether we can externally subscribe metrics to <see cref="DefaultMetricsSourceImpl"/> 
        /// and then unsubscribe.
        /// </summary>
        [Fact]
        public void TestDefaultMetricsSourceExternalMetricsSubscription()
        {
            const string evalId = "evalId";
            const string taskId = "taskId";
            const string sourceContext = "test";
            const string recordName = "testrecord";
            const string subscribedCounterName = "subscribedcounter";
            const long subscribedCounterValue = 1001;

            DefaultMetricsSourceImpl metricsSource = TangFactory.GetTang().NewInjector(
                GenerateMetricsSourceConfiguration(evalId, taskId, sourceContext, recordName))
                .GetInstance<DefaultMetricsSourceImpl>();

            IMetricsFactory factory = TangFactory.GetTang().NewInjector().GetInstance<IMetricsFactory>();
            MetricTestUtils.MetricsCollectorTestImpl collector = new MetricTestUtils.MetricsCollectorTestImpl();
         
            var extraCounter = factory.CreateCounter(subscribedCounterName,
                subscribedCounterName,
                subscribedCounterValue);
            extraCounter.Subscribe(metricsSource);

            metricsSource.GetMetrics(collector, true);
            var rb = collector.CurrentRecordBuilder;
            rb.Validate(subscribedCounterName, subscribedCounterValue);

            extraCounter.Increment();
            extraCounter.OnCompleted(); // This should remove the counter from the metrics source.
            metricsSource.GetMetrics(collector, true);
            rb = collector.CurrentRecordBuilder;
            Assert.False(rb.LongKeyPresent(subscribedCounterName), "The counter should not be present now in the source");
        }

        /// <summary>
        /// Tests whether exceptions are thrown while trying to access not created 
        /// metrics in <see cref="DefaultMetricsSourceImpl"/>
        /// </summary>
        [Fact]
        public void TestDefaultMetricsSourceInvalidAccessExceptions()
        {
            const string evalId = "evalId";
            const string taskId = "taskId";
            const string sourceContext = "test";
            const string recordName = "testrecord";
           
            DefaultMetricsSourceImpl metricsSource = TangFactory.GetTang().NewInjector(
                GenerateMetricsSourceConfiguration(evalId, taskId, sourceContext, recordName))
                .GetInstance<DefaultMetricsSourceImpl>();
    
            // Checks that exception is thrown while trying to get metrics that are not created.
            string inValidName = "invalid";
            Assert.Throws<MetricsException>(() => metricsSource.Counters[inValidName]);
            Assert.Throws<MetricsException>(() => metricsSource.DoubleGauges[inValidName]);
            Assert.Throws<MetricsException>(() => metricsSource.LongGauges[inValidName]);
            Assert.Throws<MetricsException>(() => metricsSource.Rates[inValidName]);
        }

        /// <summary>
        /// Tests whether exceptions are thrown while trying to recreate already 
        /// existing metrics in <see cref="DefaultMetricsSourceImpl"/>
        /// </summary>
        [Fact]
        public void TestDefaultMetricsSourceRecreationExceptions()
        {
            const string evalId = "evalId";
            const string taskId = "taskId";
            const string sourceContext = "test";
            const string recordName = "testrecord";
            string counterName = "cname1";
            string longGaugeName = "lname1";
            string doubleGaugeName = "dname1";
            const string rateName = "rname1";

            DefaultMetricsSourceImpl metricsSource = TangFactory.GetTang().NewInjector(
                GenerateMetricsSourceConfiguration(evalId, taskId, sourceContext, recordName))
                .GetInstance<DefaultMetricsSourceImpl>();

            metricsSource.Counters.Create(counterName, counterName);
            metricsSource.LongGauges.Create(longGaugeName, longGaugeName);
            metricsSource.DoubleGauges.Create(doubleGaugeName, doubleGaugeName);
            metricsSource.Rates.Create(rateName, rateName);

            Assert.Throws<MetricsException>(() => metricsSource.Counters.Create(counterName, counterName));
            Assert.Throws<MetricsException>(
                () => metricsSource.DoubleGauges.Create(doubleGaugeName, doubleGaugeName));
            Assert.Throws<MetricsException>(
                () => metricsSource.LongGauges.Create(longGaugeName, longGaugeName));
            Assert.Throws<MetricsException>(() => metricsSource.Rates.Create(rateName, rateName));
        }

        /// <summary>
        /// Tests whether metrics are properly deleted from <see cref="DefaultMetricsSourceImpl"/>
        /// </summary>
        [Fact]
        public void TestDefaultMetricsSourceMetricDeletion()
        {
            const string evalId = "evalId";
            const string taskId = "taskId";
            const string sourceContext = "test";
            const string recordName = "testrecord";
            string counterName = "cname1";
            string longGaugeName = "lname1";
            string doubleGaugeName = "dname1";
            const string rateName = "rname1";

            DefaultMetricsSourceImpl metricsSource = TangFactory.GetTang().NewInjector(
                GenerateMetricsSourceConfiguration(evalId, taskId, sourceContext, recordName))
                .GetInstance<DefaultMetricsSourceImpl>();

            metricsSource.Counters.Create(counterName, counterName);
            metricsSource.LongGauges.Create(longGaugeName, longGaugeName);
            metricsSource.DoubleGauges.Create(doubleGaugeName, doubleGaugeName);
            metricsSource.Rates.Create(rateName, rateName);

            MetricTestUtils.MetricsCollectorTestImpl collector = new MetricTestUtils.MetricsCollectorTestImpl();

            metricsSource.Counters.Delete(counterName);
            Assert.Throws<MetricsException>(() => metricsSource.Counters[counterName]);
            metricsSource.GetMetrics(collector, true);
            var rb = collector.CurrentRecordBuilder;
            Assert.False(rb.LongKeyPresent(counterName), "Counter was removed, so it should not be present");
        }

        /// <summary>
        ///  Verifies that record builder does not get old stats if the flag is set to false. 
        /// </summary>
        [Fact]
        public void TestDefaultMetricsSourceGetMetricsFlag()
        {
            const string evalId = "evalId";
            const string taskId = "taskId";
            const string sourceContext = "test";
            const string recordName = "testrecord";
            string[] counterNames = { "cname1", "cname2" };
 
            DefaultMetricsSourceImpl metricsSource = TangFactory.GetTang().NewInjector(
                GenerateMetricsSourceConfiguration(evalId, taskId, sourceContext, recordName))
                .GetInstance<DefaultMetricsSourceImpl>();

            MetricTestUtils.MetricsCollectorTestImpl collector = new MetricTestUtils.MetricsCollectorTestImpl();

            // Create Counters
            foreach (var name in counterNames)
            {
                metricsSource.Counters.Create(name, name);
            }

            metricsSource.GetMetrics(collector, true);

            // Do not increment second counter.
            metricsSource.Counters[counterNames[0]].Increment();
            
            // Verifies that record builder does not get old stats if the flag is set to false. 
            // Only first counter should be present.
            metricsSource.GetMetrics(collector, false);
            var rb = collector.CurrentRecordBuilder;
            Assert.True(rb.LongKeyPresent(counterNames[0]), "Counter was updated, so it should be present");
            Assert.False(rb.LongKeyPresent(counterNames[1]), "Counter was not updated, so it should not be present");
        }

        /// <summary>
        /// Tests inserting counters, gauges, rates in one go to <see cref="DefaultMetricsSourceImpl"/> 
        /// to make sure there are no across container bugs.
        /// </summary>
        [Fact]
        public void TestDefaultMetricsSourceHetergeneousFunctionalities()
        {
            const string evalId = "evalId";
            const string taskId = "taskId";
            const string sourceContext = "test";
            const string recordName = "testrecord";
            string[] counterNames = { "cname1", "cname2" };
            string[] longGaugeNames = { "lname1", "lname2" };
            string[] doubleGaugeNames = { "dname1", "dname2" };
            const string rateName = "rname1";
            double[] samples = { 2, 3 };
            string[] tagNames = { "tname1", "tname2" };
            string[] tagValues = { "tvalue1", "tValue2" };
            const long counterIncr = 2;
            const long longGaugeIncr = 3;
            const double doubleGaugeDecr = 1.2;
            const string subscribedCounterName = "subscribedcounter";
            const long subscribedCounterValue = 1001;

            DefaultMetricsSourceImpl metricsSource = TangFactory.GetTang().NewInjector(
                GenerateMetricsSourceConfiguration(evalId, taskId, sourceContext, recordName))
                .GetInstance<DefaultMetricsSourceImpl>();

            IMetricsFactory factory = TangFactory.GetTang().NewInjector().GetInstance<IMetricsFactory>();

            MetricTestUtils.MetricsCollectorTestImpl collector = new MetricTestUtils.MetricsCollectorTestImpl();

            // Create Counters
            foreach (var name in counterNames)
            {
                metricsSource.Counters.Create(name, name);
            }

            // Create long gauge
            foreach (var name in longGaugeNames)
            {
                metricsSource.LongGauges.Create(name, name);
            }

            // Create double gauge
            foreach (var name in doubleGaugeNames)
            {
                metricsSource.DoubleGauges.Create(name, name);
            }

            // Create rate.
            metricsSource.Rates.Create(rateName, rateName);

            // Update counters.
            int counter = 1;
            foreach (var name in counterNames)
            {
                metricsSource.Counters[name].Increment(counter * counterIncr);
                counter++;
            }

            // Update long gauge
            counter = 1;
            foreach (var name in longGaugeNames)
            {
                metricsSource.LongGauges[name].Increment(counter * longGaugeIncr);
                counter++;
            }

            // Update double gauge
            counter = 1;
            foreach (var name in doubleGaugeNames)
            {
                metricsSource.DoubleGauges[name].Decrement(counter * doubleGaugeDecr);
                counter++;
            }

            // Update rate
            foreach (var sample in samples)
            {
                metricsSource.Rates[rateName].Sample(sample);
            }

            // Create tags
            for (int i = 0; i < tagNames.Length; i++)
            {
                metricsSource.AddTag(tagNames[i], tagNames[i], tagValues[i]);
            }

            var extraCounter = factory.CreateCounter(subscribedCounterName,
                subscribedCounterName,
                subscribedCounterValue);
            extraCounter.Subscribe(metricsSource);

            metricsSource.GetMetrics(collector, true);
            var rb = collector.CurrentRecordBuilder;

            // Validate counters
            counter = 1;
            foreach (var name in counterNames)
            {
                rb.Validate(name, counter * counterIncr);
                counter++;
            }
            rb.Validate(subscribedCounterName, subscribedCounterValue);

            // Validate long gauges
            counter = 1;
            foreach (var name in longGaugeNames)
            {
                rb.Validate(name, counter * longGaugeIncr);
                counter++;
            }

            // Validate double gauges
            counter = 1;
            foreach (var name in doubleGaugeNames)
            {
                rb.Validate(name, -counter * doubleGaugeDecr, 1e-10);
                counter++;
            }

            // Validate tags
            for (int i = 0; i < tagNames.Length; i++)
            {
                rb.Validate(tagNames[i], tagValues[i]);
            }

            // Validate rate
            rb.Validate(rateName + "-Num", samples.Length);
            rb.Validate(rateName + "-RunningAvg", samples.Sum() / samples.Length, 1e-10);         
        }

        public static IConfiguration GenerateMetricsSourceConfiguration(string evalId, string taskId, string sourceContext, string recordName)
        {
            return
                DefaultMetricsSourceConfiguration.ConfigurationModule.Set(DefaultMetricsSourceConfiguration.EvaluatorId,
                    evalId)
                    .Set(DefaultMetricsSourceConfiguration.TaskOrContextId, taskId)
                    .Set(DefaultMetricsSourceConfiguration.RecordId, recordName)
                    .Set(DefaultMetricsSourceConfiguration.SourceContext, sourceContext)
                    .Build();
        }
    }
}
