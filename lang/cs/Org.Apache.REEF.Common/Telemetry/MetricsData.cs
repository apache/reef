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
        private ConcurrentDictionary<string, MetricData> _metricsMap = new ConcurrentDictionary<string, MetricData>();

        /// <summary>
        /// The lock for metrics.
        /// </summary>
        private readonly object _metricLock = new object();

        [Inject]
        private MetricsData()
        {
        }

        /// <summary>
        /// Deserialization.
        /// </summary>
        /// <param name="serializedMetricsString"></param>
        [JsonConstructor]
        internal MetricsData(string serializedMetricsString)
        {
            var metrics = JsonConvert.DeserializeObject<IList<MetricData>>(serializedMetricsString, settings);
            foreach (var m in metrics)
            {
                _metricsMap.TryAdd(m.GetMetric().Name, m);
            }
        }

        internal MetricsData(IMetrics metrics)
        {
            foreach (var me in metrics.GetMetrics())
            {
                _metricsMap.TryAdd(me.GetMetric().Name, new MetricData(me.GetMetric()));
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
            if (!_metricsMap.TryAdd(metric.Name, new MetricData(metric)))
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
            if (!_metricsMap.TryGetValue(name, out MetricData md))
            {
                me = null;
                return false;
            }
            me = md.GetMetric();
            return true;
        }

        /// <summary>
        /// Gets all the registered metrics.
        /// </summary>
        /// <returns>IEnumerable of MetricData.</returns>
        public IEnumerable<MetricData> GetMetrics()
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
            lock (_metricLock)
            {
                foreach (var metric in metrics.GetMetrics())
                {
                    _metricsMap.AddOrUpdate(metric.GetMetric().Name, metric, (k, v) => metric);
                }
            }
        }

        /// <summary>
        /// Updates metrics with a single metric object. If the metric has already been registered,
        /// update the value; if not, add it to the registry.
        /// </summary>
        /// <param name="me">New metric object to be added or updated.</param>
        internal void Update(IMetric me)
        {
            lock (_metricLock)
            {
                _metricsMap.AddOrUpdate(me.Name, new MetricData(me), (k, v) => );


                if (_metricsMap.TryGetValue(me.Name, out MetricData metricData))
                {
                    metricData.UpdateMetric(me);
                }
                else
                {
                    _metricsMap.Add(me.Name, new MetricData(me));
                }
            }
        }

        /// <summary>
        /// Updates the metric to the specified value given the metric's name. If the metric name has been registered,
        /// update the value; if not, the metric is not added and an exception is thrown.
        /// </summary>
        /// <param name="name">Name of the metric to update.</param>
        /// <param name="val">New for the specified metric.</param>
        internal void Update(string name, object val)
        {
            lock (_metricLock)
            {
                if (_metricsMap.TryGetValue(name, out MetricData me))
                {
                    me.UpdateMetric(name, val);
                }
                else
                {
                    Logger.Log(Level.Error, "Metric {0} needs to be registered before it can be updated with value {1}.", name, val);
                    throw new Exception("Metric " + name + " has not been registered.");
                }
            }
        }

        /// <summary>
        /// Reset changed since last sink for each metric
        /// </summary>
        internal void Reset()
        {
            lock (_metricLock)
            {
                foreach (var c in _metricsMap.Values)
                {
                    c.ResetChangeSinceLastSink();
                }
            }
        }

        /// <summary>
        /// Convert the metric data to a collection of IMetric for sinking.
        /// </summary>
        /// <returns>A collection of metric records.</returns>
        internal IEnumerable<IMetric> GetMetricsHistory()
        {
            return _metricsMap.Select(metric => metric.Value.GetMetricRecords()).SelectMany(r => r);
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
            lock (_metricLock)
            {
                if (_metricsMap.Count > 0)
                {
                    return JsonConvert.SerializeObject(_metricsMap.Values.Where(me => me.ChangesSinceLastSink > 0).ToList(), settings);
                }
            }
            return null;
        }
    }
}
