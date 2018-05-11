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
using Newtonsoft.Json;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Telemetry
{
    /// <summary>
    /// MetricData class maintains the current value of a single metric and keeps count of the number
    /// of times this metric has been updated. If the metric is immutable, it keeps a record of updates.
    /// Once the data has been processed, the records and count will reset.
    /// </summary>
    [JsonObject]
    public sealed class MetricData
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MetricsData));

        /// <summary>
        /// Metric object
        /// </summary>
        [JsonProperty]
        private IMetric _metric;

        /// <summary>
        /// List of all udpated values since last processed, including current.
        /// </summary>
        [JsonProperty]
        private IList<IMetric> _records;

        /// <summary>
        /// Number of times metric has been updated since last processed.
        /// </summary>
        [JsonProperty]
        internal int ChangesSinceLastSink;

        /// <summary>
        /// Constructor for metricData
        /// </summary>
        /// <param name="metric"></param>
        /// <param name="initialValue"></param>
        internal MetricData(IMetric metric)
        {
            _metric = metric;
            ChangesSinceLastSink = 0;
            _records = new List<IMetric>();
            _records.Add(_metric);
        }

        [JsonConstructor]
        internal MetricData(IMetric metric, IList<IMetric> records, int changes)
        {
            _metric = metric;
            _records = records;
            ChangesSinceLastSink = changes;
        }

        /// <summary>
        /// Reset records.
        /// </summary>
        internal void ResetChangeSinceLastSink()
        {
            ChangesSinceLastSink = 0;
            _records.Clear();
        }

        /// <summary>
        /// When new metric data is received, update the value and records so it reflects the new data.
        /// </summary>
        /// <param name="metric">Metric data received.</param>
        internal void UpdateMetric(MetricData metric)
        {
            ChangesSinceLastSink++;
            _metric = metric.GetMetric();
            if (metric.GetMetric().IsImmutable && metric.ChangesSinceLastSink > 0)
            {
                foreach (var r in metric._records)
                {
                    _records.Add(r);
                }
            }
            ChangesSinceLastSink += metric.ChangesSinceLastSink;
        }

        /// <summary>
        /// Updates metric value with metric object received.
        /// </summary>
        /// <param name="me">New metric.</param>
        internal void UpdateMetric(IMetric me)
        {
            if (me.GetType() != _metric.GetType())
            {
                throw new ApplicationException("Trying to update metric of type " + _metric.GetType() + " with type " + me.GetType());
            }
            ChangesSinceLastSink++;
            _metric = me;
            if (_metric.IsImmutable)
            {
                _records.Add(_metric);
            }
        }

        /// <summary>
        /// Updates metric value given its name.
        /// </summary>
        /// <param name="name">Name of the metric to update.</param>
        /// <param name="val">New value.</param>
        internal void UpdateMetric(string name, object val)
        {
            ChangesSinceLastSink++;
            _metric = _metric.CreateInstanceWithNewValue(val);
            if (_metric.IsImmutable)
            {
                _records.Add(_metric);
            }
        }

        /// <summary>
        /// Get the metric with its most recent value.
        /// </summary>
        /// <returns></returns>
        internal IMetric GetMetric()
        {
            return _metric;
        }

        /// <summary>
        /// Get KeyValuePair for every record and current metric value.
        /// </summary>
        /// <returns>This metric's values.</returns>
        internal IEnumerable<IMetric> GetMetricRecords()
        {
            var values = new List<IMetric>();

            if (_metric.IsImmutable)
            {
                values.AddRange(_records.Select(r => r));
            }
            else
            {
                values.Add(_metric);
            }
            return values;
        }
    }
}
