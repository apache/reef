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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Org.Apache.REEF.Tang.Annotations;
using Newtonsoft.Json;

namespace Org.Apache.REEF.Common.Telemetry
{
    /// <summary>
    /// This class maintains a collection of the data for all the metrics for metrics service.
    /// When new metric data is received, the data in the collection will be updated.
    /// After the data is processed, the changes since last process will be reset.
    /// </summary>
    public sealed class MetricsData : IMetricSet
    {
        private static readonly JsonSerializerSettings settings = new JsonSerializerSettings()
        {
            TypeNameHandling = TypeNameHandling.All
        };

        /// <summary>
        /// Registration of metrics
        /// </summary>
        private readonly ConcurrentDictionary<string, MetricTracker> _metricsMap =
            new ConcurrentDictionary<string, MetricTracker>();

        [Inject]
        internal MetricsData()
        {
        }

        /// <summary>
        /// Deserialization.
        /// </summary>
        /// <param name="serializedMetricsString">string to Deserialize.</param>
        [JsonConstructor]
        internal MetricsData(string serializedMetricsString)
        {
            var metrics = JsonConvert.DeserializeObject<IEnumerable<MetricTracker>>(
                serializedMetricsString,
                settings);

            foreach (var m in metrics)
            {
                _metricsMap.TryAdd(m.MetricName, m);
            }
        }

        public void RegisterMetric(IMetric metric)
        {
            if (!_metricsMap.TryAdd(metric.Name, new MetricTracker(metric)))
            {
                throw new ArgumentException("The metric [{0}] already exists.", metric.Name);
            }
        }

        public bool TryGetMetric<T>(string name, out T metric)
            where T : IMetric
        {
            bool success = _metricsMap.TryGetValue(name, out MetricTracker tracker);
            metric = (T)tracker?.GetMetric();
            return success && metric != null;
        }

        public IEnumerable<MetricTracker> GetMetricTrackers()
        {
            return _metricsMap.Values;
        }

        /// <summary>
        /// Flushes changes since last sink for each metric.
        /// Called when Driver is sinking metrics.
        /// </summary>
        /// <returns>Key value pairs of metric name and record that was flushed.</returns>
        public IEnumerable<KeyValuePair<string, MetricTracker.MetricRecord>> FlushMetricRecords()
        {
            // for each metric, flush the records and create key value pairs
            return _metricsMap.SelectMany(
                kv => kv.Value.FlushRecordsCache().Select(
                    r => new KeyValuePair<string, MetricTracker.MetricRecord>(kv.Key, r)));
        }

        /// <summary>
        /// Updates metrics given another <see cref="MetricsData"/> object.
        /// For every metric in the new set, if it is registered then update the value,
        /// if it is not then add it to the registration.
        /// </summary>
        /// <param name="metrics">New metric values to be updated.</param>
        internal void Update(IMetricSet metrics)
        {
            foreach (var tracker in metrics.GetMetricTrackers())
            {
                _metricsMap.AddOrUpdate(
                    tracker.MetricName,
                    tracker,
                    (k, v) => v.UpdateMetric(tracker));
            }
        }

        /// <summary>
        /// Flushes that trackers contained in the queue.
        /// Called when Evaluator is sending metrics information to Driver.
        /// </summary>
        /// <returns>Queue of trackers containing metric records.</returns>
        internal IEnumerable<MetricTracker> FlushMetricTrackers()
        {
            return new ConcurrentQueue<MetricTracker>(_metricsMap.Select(
                kv => new MetricTracker(
                    kv.Value.MetricName,
                    kv.Value.FlushRecordsCache(),
                    kv.Value.KeepUpdateHistory)));
        }

        /// <summary>
        /// Sums up the total changes to metrics to see if it has reached the sink threshold.
        /// </summary>
        /// <returns>Returns whether the sink threshold has been met.</returns>
        internal bool TriggerSink(int metricSinkThreshold)
        {
            return _metricsMap.Values.Sum(e => e.GetRecordCount()) > metricSinkThreshold;
        }

        public string Serialize()
        {
            return Serialize(_metricsMap.Values);
        }

        internal string Serialize(IEnumerable<MetricTracker> trackers)
        {
            return JsonConvert.SerializeObject(trackers, settings);
        }

        internal string SerializeAndReset()
        {
            return Serialize(FlushMetricTrackers());
        }
    }
}
