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
using System.Threading;
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
    public sealed class MetricTracker : IObserver<IMetric>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MetricsData));

        private IDisposable _unsubscriber;

        [JsonProperty]
        private IMetric _metric;

        [JsonProperty]
        internal bool KeepUpdateHistory;

        /// <summary>
        /// List of the history of values this metric has held. If _keepUpdateHistory is false, only holds current value.
        /// </summary>
        [JsonProperty]
        private ConcurrentQueue<MetricRecord> _records;
        
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
        internal MetricTracker(IMetric metric)
        {
            Subscribe(metric);
            ChangesSinceLastSink = 0;
            KeepUpdateHistory = metric.IsImmutable;
            _records = new ConcurrentQueue<MetricRecord>();
            if (KeepUpdateHistory)
            {
                _records.Enqueue(CreateMetricRecord(metric));
            }
        }

        [JsonConstructor]
        internal MetricTracker(IMetric _mirRor, int changesSinceLastSink, ConcurrentQueue<MetricRecord> _recorDs, bool keepUpdateHistory)
        {
            _metric = _mirRor;
            // _records = new ConcurrentQueue<MetricRecord>(_recorDs);
            _records = _recorDs;
            KeepUpdateHistory = keepUpdateHistory;
            ChangesSinceLastSink = changesSinceLastSink;
        }

        private MetricTracker(IMetric mirror, ConcurrentQueue<MetricRecord> records, int changes, bool history)
        {
            _metric = mirror;
            _records = records;
            ChangesSinceLastSink = changes;
            KeepUpdateHistory = history;
        }

        /// <summary>
        /// Flush records.
        /// </summary>
        internal ConcurrentQueue<MetricRecord> FlushChangesSinceLastSink()
        {
            ConcurrentQueue<MetricRecord> records = new ConcurrentQueue<MetricRecord>();
            ////if (KeepUpdateHistory)
            if(!_records.IsEmpty)
            {
                MetricRecord record;
                while (_records.TryDequeue(out record))
                {
                    records.Enqueue(record);
                }
            }
            else
            {
                records.Enqueue(CreateMetricRecord(_metric));
            }
            ChangesSinceLastSink = 0;
            return records;
        }

        /// <summary>
        /// When new metric data is received, update the value and records so it reflects the new data.
        /// Called when driver receives metrics from evaluator.
        /// </summary>
        /// <param name="metric">Metric data received.</param>
        internal MetricTracker UpdateMetric(MetricTracker metric)
        {
            if (metric.ChangesSinceLastSink > 0)
            {
                if (KeepUpdateHistory)
                {
                    var recordsToAdd = metric.GetMetricRecords();
                    while(recordsToAdd.TryDequeue(out MetricRecord record))
                    {
                        _records.Enqueue(record);
                    }
                }
                else
                {
                    Logger.Log(Level.Info, "There should only be one record: {0} records - {1}", metric._records.Count(), metric._records.First().Value);
                    // Logger.Log(Level.Info, "Exchanging old record {0} for new value {1}", _records.First().Value, metric.GetMetricRecords().First().Value);
                    _records = metric.GetMetricRecords();
                    Logger.Log(Level.Info, "Record exchange complete: size {0} value {1}", _records.Count(), _records.First().Value);
                    // Interlocked.Exchange(ref _records, metric.GetMetricRecords());
                }
            }
            ChangesSinceLastSink += metric.ChangesSinceLastSink;
            return this;
        }

        /// <summary>
        /// Updates metric value with metric object received.
        /// </summary>
        /// <param name="me">New metric.</param>
        internal void UpdateMetric(IMetric me)
        {
            ChangesSinceLastSink++;
            if (KeepUpdateHistory)
            {
                _records.Enqueue(CreateMetricRecord(_metric));
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
        /// Get all the metric records.
        /// </summary>
        /// <returns>The history of the metric values.</returns>
        internal ConcurrentQueue<MetricRecord> GetMetricRecords()
        {
            if (_records.IsEmpty)
            {
                var currentValueQ = new ConcurrentQueue<MetricRecord>();
                currentValueQ.Enqueue(CreateMetricRecord(_metric));
                return currentValueQ;
            }
            else
            {
                return _records;
            }
        }

        public void Subscribe(IMetric provider)
        {
            _metric = provider;
            _unsubscriber = provider.Subscribe(this);
        }

        public void Unsubscribe()
        {
            _unsubscriber.Dispose();
        }

        public void OnNext(IMetric metric)
        {
            UpdateMetric(metric);
        }

        public void OnError(Exception error)
        {
            Logger.Log(Level.Info, error.Message);
        }

        public void OnCompleted()
        {
        }

        private MetricRecord CreateMetricRecord(IMetric metric)
        {
            return new MetricRecord(metric);
        }

        [JsonObject]
        public struct MetricRecord
        {
            [JsonProperty]
            public object Value { get; }

            [JsonProperty]
            public long Timestamp { get; }

            [JsonConstructor]
            public MetricRecord(object value, long timestamp)
            {
                Value = value;
                Timestamp = timestamp;
            }

            public MetricRecord(IMetric metric)
            {
                Timestamp = metric.Timestamp;
                Value = metric.ValueUntyped;
            }
        }
    }
}
