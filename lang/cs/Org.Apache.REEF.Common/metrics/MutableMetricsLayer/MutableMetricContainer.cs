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

namespace Org.Apache.REEF.Common.Metrics.MutableMetricsLayer
{
    /// <summary>
    /// A container containing collection of particular kind of mutable metrics
    /// like counters, gauge etc.
    /// </summary>
    /// <typeparam name="T">Type of mutable metric.</typeparam>
    internal sealed class MutableMetricContainer<T> : IMetricContainer<T> where T : IMutableMetric
    {
        private readonly Dictionary<string, T> _metricsDictionary = new Dictionary<string, T>();
        private readonly Func<string, string, T> _createMetricFromNameDesc;
        private readonly IObservable<SnapshotRequest> _provider;
        private readonly object _lock = new object();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="metricFromNameDesc">A function that creates mutable metric from name and desc.</param>
        /// <param name="provider">Observale to which new metric should subscribe to.</param>
        internal MutableMetricContainer(Func<string, string, T> metricFromNameDesc,
            IObservable<SnapshotRequest> provider)
        {
            _createMetricFromNameDesc = metricFromNameDesc;
            _provider = provider;
        }

        /// <summary>
        /// Creates a metric from name and description. Throws exception if
        /// metric with given name already exists.
        /// </summary>
        /// <param name="name">Name of the metric.</param>
        /// <param name="desc">Description of the metric.</param>
        /// <returns>Newly created metric.</returns>
        public T Create(string name, string desc)
        {
            lock (_lock)
            {
                if (!_metricsDictionary.ContainsKey(name))
                {
                    _metricsDictionary[name] = _createMetricFromNameDesc(name, desc);
                    _metricsDictionary[name].Subscribe(_provider);
                    return _metricsDictionary[name];
                }
                throw new MetricsException(string.Format("Metric with name {0} already exists", name));
            }
        }

        /// <summary>
        /// Deletes a metric with given name and description. 
        /// </summary>
        /// <param name="name">Name of the metric.</param>
        /// <returns>True if the metric is deleted, false if it does not exist</returns>
        public bool Delete(string name)
        {
            T entry;
            lock (_lock)
            {
                if (!_metricsDictionary.ContainsKey(name))
                {
                    return false;
                }
                entry = _metricsDictionary[name];
                _metricsDictionary.Remove(name);
            }
            entry.OnCompleted();
            return true;
        }

        /// <summary>
        /// String based indexer. Gets metric by name. Throws exception 
        /// if it does not exist. 
        /// </summary>
        /// <param name="name">Name of the metric,</param>
        /// <returns>Existing metric with given name.</returns>
        public T this[string name]
        {
            get
            {
                lock (_lock)
                {
                    if (!_metricsDictionary.ContainsKey(name))
                    {
                        throw new MetricsException(string.Format("Metric with name {0} does not exist", name));
                    }
                    return _metricsDictionary[name];
                }
            }
        }
    }
}
