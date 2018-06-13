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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
using Newtonsoft.Json;

namespace Org.Apache.REEF.Common.Telemetry
{
    /// <summary>
    /// This class maintains a collection of the data for all the metrics for metrics service. 
    /// When new metric data is received, the data in the collection will be updated.
    /// After the data is processed, the changes since last process will be reset.
    /// </summary>
    public sealed class MetricsData : IMetrics
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MetricsData));

        JsonSerializerSettings settings = new JsonSerializerSettings()
        {
            TypeNameHandling = TypeNameHandling.All
        };

        /// <summary>
        /// Registration of metrics
        /// </summary>
        private ConcurrentDictionary<string, MetricTracker> _metricsMap = new ConcurrentDictionary<string, MetricTracker>();

        [Inject]
        internal MetricsData()
        {
        }

        /// <summary>
        /// Deserialization.
        /// </summary>
        /// <param name="serializedMetricsString"></param>
        [JsonConstructor]
        internal MetricsData(string serializedMetricsString)
        {
            var metrics = JsonConvert.DeserializeObject<IList<MetricTracker>>(serializedMetricsString, settings);

            foreach (var m in metrics)
            {
                _metricsMap.TryAdd(m.GetMetric().Name, m);
            }
        }

        internal MetricsData(IMetrics metrics)
        {
            foreach (var me in metrics.GetMetrics())
            {
                _metricsMap.TryAdd(me.GetMetric().Name, new MetricTracker(me.GetMetric()));
            }
        }

        /// <summary>
        /// Checks if the metric to be registered has a unique name. If the metric name has already been 
        /// registered, metric is not entered into the registration and method returns false. On successful
        /// registration, method returns true.
        /// </summary>
        /// <param name="metric">Metric to register.</param>
        /// <returns>Indicates if the metric was registered.</returns>
        public bool TryRegisterMetric(IMetric metric)
        {
            if (!_metricsMap.TryAdd(metric.Name, new MetricTracker(metric)))
            {
                Logger.Log(Level.Warning, "The metric [{0}] already exists.", metric.Name);
                return false;
            }
            return true;
        }

        /// <summary>
        /// Gets a metric given a name.
        /// </summary>
        /// <param name="name">Name of the metric.</param>
        /// <param name="me">The metric object returned.</param>
        /// <returns>Boolean indicating if a metric object was succesfully retrieved.</returns>
        public bool TryGetValue(string name, out IMetric me)
        {
            if (!_metricsMap.TryGetValue(name, out MetricTracker tracker))
            {
                me = null;
                return false;
            }
            me = tracker.GetMetric();
            return true;
        }

        /// <summary>
        /// Gets all the registered metrics.
        /// </summary>
        /// <returns>IEnumerable of MetricData.</returns>
        public IEnumerable<MetricTracker> GetMetrics()
        {
            return _metricsMap.Values;
        }

        /// <summary>
        /// Updates metrics given another <see cref="MetricsData"/> object.
        /// For every metric in the new set, if it is registered then update the value,
        /// if it is not then add it to the registration.
        /// </summary>
        /// <param name="metrics">New metric values to be updated.</param>
        internal void Update(MetricsData metrics)
        {
            foreach (var metric in metrics.GetMetrics())
            {
                _metricsMap.AddOrUpdate(metric.GetMetric().Name, metric, (k, v) => v.UpdateMetric(metric));
            }
        }

        /// <summary>
        /// Flush changes since last sink for each metric. Called when driver is sinking metrics.
        /// </summary>
        public IEnumerable<KeyValuePair<string, MetricTracker.MetricRecord>> FlushMetricRecords()
        {
            // for each metric, flush the records and create key value pairs
            return _metricsMap.SelectMany(kv => kv.Value.FlushChangesSinceLastSink().Select(r => new KeyValuePair<string, MetricTracker.MetricRecord>(kv.Key, r)));
        }

        /// <summary>
        /// Called when evaluator is sending metrics information to driver
        /// </summary>
        /// <returns></returns>
        public ConcurrentQueue<MetricTracker> FlushMetricTrackers()
        {
            return new ConcurrentQueue<MetricTracker>(_metricsMap.Select(kv => new MetricTracker(kv.Value.GetMetric(), kv.Value.ChangesSinceLastSink, kv.Value.FlushChangesSinceLastSink(), kv.Value.KeepUpdateHistory)));
        }

        /// <summary>
        /// The condition that triggers the sink. The condition can be modified later.
        /// </summary>
        /// <returns></returns>
        internal bool TriggerSink(int metricSinkThreshold)
        {
            return _metricsMap.Values.Sum(e => e.ChangesSinceLastSink) > metricSinkThreshold;
        }

        public string Serialize()
        {
            return Serialize(_metricsMap.Values);
        }

        internal string Serialize(IEnumerable<MetricTracker> trackers)
        {
            return JsonConvert.SerializeObject(trackers.Where(me => me.ChangesSinceLastSink > 0).ToList(), settings);
        }

        internal string SerializeAndReset()
        {
            return Serialize(FlushMetricTrackers());
        }
    }
}
