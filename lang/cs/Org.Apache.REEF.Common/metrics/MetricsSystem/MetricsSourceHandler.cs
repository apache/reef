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

using Org.Apache.REEF.Common.Metrics.Api;

namespace Org.Apache.REEF.Common.Metrics.MetricsSystem
{
    /// <summary>
    /// Helper class handling the <see cref="IMetricsSource"/>. Metrics system uses this 
    /// class for collecting metrics from the source. The class is not thread safe 
    /// and caller is supposed to maintain the safety.
    /// </summary>
    internal sealed class MetricsSourceHandler
    {
        private readonly IMetricsSource _source;
 
        /// <summary>
        /// Creates the handler class for the source.
        /// </summary>
        /// <param name="name">Name of the source.</param>
        /// <param name="source">Actual source object.</param>
        public MetricsSourceHandler(string name, IMetricsSource source)
        {
            Name = name;
            _source = source;
        }

        /// <summary>
        /// Underlying <see cref="IMetricsSource"/> object.
        /// </summary>
        public IMetricsSource Source
        {
            get { return _source; }
        }

        /// <summary>
        /// Name of the source.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Gets the metrics from the source.
        /// </summary>
        /// <param name="collector">Collector to get the snapshot.</param>
        /// <param name="all">Whether to get metrics that did not change.</param>
        /// <returns>A snapshot object to be consumed by sinks.</returns>
        public SourceSnapshot GetMetrics(IMetricsCollector collector, bool all)
        {
            _source.GetMetrics(collector, all);
            var records = collector.GetRecords();
            return new SourceSnapshot(Name, records);
        } 
    }
}
