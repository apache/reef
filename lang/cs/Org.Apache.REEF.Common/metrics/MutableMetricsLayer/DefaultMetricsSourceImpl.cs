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
using Org.Apache.REEF.Common.Metrics.Api;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Metrics.MutableMetricsLayer
{
    /// <summary>
    /// Default implementation of <see cref="IMetricsSource"/>. Contains user friendly aceess to 
    /// add and update metrics. Can be extended or totally ignored by the user.
    /// </summary>
    [Unstable("0.16", "Contract may change.")]
    public class DefaultMetricsSourceImpl : IMetricsSource, IDisposable
    {
        private readonly IMetricContainer<ICounter> _counters;
        private readonly IMetricContainer<ILongGauge> _longGauges;
        private readonly IMetricContainer<IDoubleGauge> _doubleGauges;
        private readonly IMetricContainer<IRate> _rates;
        private readonly Dictionary<string, MetricsTag> _tags = new Dictionary<string, MetricsTag>();

        private readonly IList<IObserver<SnapshotRequest>> _observers = new List<IObserver<SnapshotRequest>>();
        private readonly object _lock = new object();
        private readonly string _contextOrTaskName;
        private readonly string _evaluatorId;
        private readonly string _sourceContext;
        private readonly string _recordName;
        private readonly IMetricsFactory _metricsFactory;

        [Inject]
        private DefaultMetricsSourceImpl(
            [Parameter(typeof(DefaultMetricsSourceParameters.ContextOrTaskName))] string contextOrTaskName,
            [Parameter(typeof(DefaultMetricsSourceParameters.EvaluatorId))] string evaluatorId,
            [Parameter(typeof(DefaultMetricsSourceParameters.SourceContext))] string sourceContext,
            [Parameter(typeof(DefaultMetricsSourceParameters.RecordName))] string recordName,
            IMetricsFactory metricsFactory)
        {
            _contextOrTaskName = contextOrTaskName;
            _evaluatorId = evaluatorId;
            _sourceContext = sourceContext;
            _recordName = recordName;
            _metricsFactory = metricsFactory;

            _counters = new MutableMetricContainer<ICounter>(
                (name, desc) => metricsFactory.CreateCounter(name, desc),
                this);
            _longGauges = new MutableMetricContainer<ILongGauge>(
                (name, desc) => metricsFactory.CreateLongGauge(name, desc),
                this);
            _doubleGauges = new MutableMetricContainer<IDoubleGauge>(
                (name, desc) => metricsFactory.CreateDoubleGauge(name, desc),
                this);
            _rates = new MutableMetricContainer<IRate>(
                (name, desc) => metricsFactory.CreateRateMetric(name, desc),
                this);
        }

        /// <summary>
        /// Returns indexable counters container.
        /// </summary>
        public IMetricContainer<ICounter> Counters
        {
            get { return _counters; }
        }

        /// <summary>
        /// Returns indexable long guage container.
        /// </summary>
        public IMetricContainer<ILongGauge> LongGauges
        {
            get { return _longGauges; }
        }

        /// <summary>
        /// Returns indexable double gauge container.
        /// </summary>
        public IMetricContainer<IDoubleGauge> DoubleGauges
        {
            get { return _doubleGauges; }
        }

        /// <summary>
        /// Returns indexable double gauge container.
        /// </summary>
        public IMetricContainer<IRate> Rates
        {
            get { return _rates; }
        }

        /// <summary>
        /// Adds a tag to the source. Replaces old one if it exists.
        /// </summary>
        /// <param name="name">Name of the tag.</param>
        /// <param name="desc">Description of the tag.</param>
        /// <param name="value">Value of the tag.</param>
        public void AddTag(string name, string desc, string value)
        {
            lock (_lock)
            {
                _tags[name] = _metricsFactory.CreateTag(new MetricsInfoImpl(name, desc), value);
            }
        }

        /// <summary>
        /// Returns tag by name.
        /// </summary>
        /// <param name="name">Name of the tag.</param>
        /// <returns>Tag if it exists, null otherwise.</returns>
        public MetricsTag GetTag(string name)
        {
            lock (_lock)
            {
                MetricsTag tag;
                _tags.TryGetValue(name, out tag);
                return tag;
           }         
        }

        /// <summary>
        /// Subscribes the <see cref="MutableMetricBase"/> whose snapshot 
        /// will be taken. This function will be used if user has its own metric types except 
        /// from the ones used in this class. For example, a ML algorithm can subscribe itself 
        /// as observer, and directly add metric vlaues like iterations, loss function etc. in the 
        /// record. 
        /// </summary>
        /// <param name="observer">Observer that takes snapshot of the metric.</param>
        /// <returns>A disposable handler used to unsubscribe the observer.</returns>
        public IDisposable Subscribe(IObserver<SnapshotRequest> observer)
        {
            lock (_lock)
            {
                if (!_observers.Contains(observer))
                {
                    _observers.Add(observer);
                }
                return new Unsubscriber(_observers, observer, _lock);
            }
        }

        /// <summary>
        /// Gets metrics from the source.
        /// </summary>
        /// <param name="collector">Collector that stores the resulting metrics snapshot as records.</param>
        /// <param name="all">If true, gets metric values even if they are unchanged.</param>
        public void GetMetrics(IMetricsCollector collector, bool all)
        {
            lock (_lock)
            {
                var rb = collector.CreateRecord(_recordName)
                    .SetContext(_sourceContext)
                    .AddTag("TaskOrContextName", _contextOrTaskName)
                    .AddTag("EvaluatorId", _evaluatorId)
                    .AddTag("SourceType", "DefaultSource");
                var request = new SnapshotRequest(rb, all);
                foreach (var entry in _observers)
                {
                    entry.OnNext(request);
                }

                foreach (var entry in _tags)
                {
                    rb.Add(entry.Value);
                }
            }
        }

        /// <summary>
        /// Diposes the <see cref="DefaultMetricsSourceImpl"/>.
        /// Removes all the observers.
        /// </summary>
        public void Dispose()
        {
            foreach (var observer in _observers)
            {
                observer.OnCompleted();
            }
        }

        private class Unsubscriber : IDisposable
        {
            private readonly IList<IObserver<SnapshotRequest>> _observers;
            private readonly IObserver<SnapshotRequest> _observer;
            private readonly object _lock;
            public Unsubscriber(IList<IObserver<SnapshotRequest>> observers, IObserver<SnapshotRequest> observer, object lockObject)
            {
                _observers = observers;
                _observer = observer;
                _lock = lockObject;
            }

            public void Dispose()
            {
                lock (_lock)
                {
                    if (_observer != null && _observers.Contains(_observer))
                    {
                        _observers.Remove(_observer);
                    }
                }
            }
        }
    }
}