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
    public sealed class MetricTracker : ITracker
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MetricTracker));

        [JsonProperty]
        internal IMetric Metric;

        [JsonProperty]
        internal bool KeepUpdateHistory;

        /// <summary>
        /// if KeepUpdateHistory is true, keeps a history of updates.
        /// </summary>
        [JsonProperty]
        internal ConcurrentQueue<MetricRecord> Records;
        
        /// <summary>
        /// Number of times metric has been updated since last processed.
        /// </summary>
        [JsonProperty]
        internal int ChangesSinceLastSink;

        private IDisposable _unsubscriber;

        /// <summary>
        /// Constructor for metricData
        /// </summary>
        /// <param name="metric"></param>
        /// <param name="initialValue"></param>
        internal MetricTracker(IMetric metric)
        {
            Subscribe(metric);
            ChangesSinceLastSink = 0;
            KeepUpdateHistory = metric.KeepUpdateHistory;
            Records = new ConcurrentQueue<MetricRecord>();
            if (KeepUpdateHistory)
            {
                Records.Enqueue(CreateMetricRecord(metric));
            }
        }

        [JsonConstructor]
        internal MetricTracker(IMetric metric, int changesSinceLastSink, ConcurrentQueue<MetricRecord> records, bool keepUpdateHistory)
        {
            Metric = metric;
            Records = records;
            KeepUpdateHistory = keepUpdateHistory;
            ChangesSinceLastSink = changesSinceLastSink;
        }

        /// <summary>
        /// Flush records currently held in the records queue.
        /// </summary>
        /// <returns>A queue containing all the flushed records.</returns>
        internal ConcurrentQueue<MetricRecord> FlushChangesSinceLastSink()
        {
            ConcurrentQueue<MetricRecord> records = new ConcurrentQueue<MetricRecord>();
            if(!Records.IsEmpty)
            {
                while (Records.TryDequeue(out MetricRecord record))
                {
                    records.Enqueue(record);
                }
            }
            else
            {
                // Records will be empty only on eval side when tracker doesn't keep history.
                records.Enqueue(CreateMetricRecord(Metric));
            }
            ChangesSinceLastSink = 0;
            return records;
        }

        /// <summary>
        /// When new metric data is received, update the value and records so it reflects the new data.
        /// Called when Driver receives metrics from Evaluator.
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
                        Records.Enqueue(record);
                    }
                }
                else
                {
                    Interlocked.Exchange(ref Records, metric.GetMetricRecords());
                }
            }
            ChangesSinceLastSink += metric.ChangesSinceLastSink;
            return this;
        }

        /// <summary>
        /// Get the metric with its most recent value.
        /// </summary>
        /// <returns></returns>
        internal IMetric GetMetric()
        {
            return Metric;
        }

        /// <summary>
        /// If KeepUpdateHistory is true, it will return all the records; otherwise, it will returen one record with the most recent value.
        /// </summary>
        /// <returns>The history of the metric records.</returns>
        internal ConcurrentQueue<MetricRecord> GetMetricRecords()
        {
            if (Records.IsEmpty)
            {
                var currentValueQ = new ConcurrentQueue<MetricRecord>();
                currentValueQ.Enqueue(CreateMetricRecord(Metric));
                return currentValueQ;
            }
            else
            {
                return Records;
            }
        }

        /// <summary>
        /// Subscribes the tracker to a metric object.
        /// </summary>
        /// <param name="provider">The metric to track.</param>
        public void Subscribe(IMetric provider)
        {
            Metric = provider;
            _unsubscriber = provider.Subscribe(this);
        }

        /// <summary>
        /// Unsubscribes the tracker from the metric it is tracking.
        /// </summary>
        public void Unsubscribe()
        {
            _unsubscriber.Dispose();
        }

        /// <summary>
        /// Creates and queues a new metric record with a value.
        /// </summary>
        /// <param name="value">Value of the new record.</param>
        public void Track(object value)
        {
            ChangesSinceLastSink++;
            if (KeepUpdateHistory)
            {
                Records.Enqueue(CreateMetricRecord(value));
            }
        }

        private MetricRecord CreateMetricRecord(IMetric metric)
        {
            return new MetricRecord(metric);
        }

        private MetricRecord CreateMetricRecord(object val)
        {
            return new MetricRecord(val);
        }

        [JsonObject]
        public class MetricRecord
        {
            private object _value;

            [JsonProperty]
            public object Value
            {
                get
                {
                    return _value;
                }
            }
            [JsonProperty]
            public long Timestamp { get; }

            [JsonConstructor]
            public MetricRecord(object value, long timestamp)
            {
                _value = value;
                Timestamp = timestamp;
            }

            public MetricRecord(IMetric metric)
            {
                Timestamp = DateTime.Now.Ticks;
                Interlocked.Exchange(ref _value, metric.ValueUntyped);
            }

            public MetricRecord(object val)
            {
                _value = val;
                Timestamp = DateTime.Now.Ticks;
            }
        }
    }
}
