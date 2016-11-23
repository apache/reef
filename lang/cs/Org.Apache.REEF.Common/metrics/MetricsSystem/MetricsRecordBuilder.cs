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

namespace Org.Apache.REEF.Common.Metrics.MetricsSystem
{
    /// <summary>
    /// Default implementation of <see cref="IMetricsRecordBuilder"/>.
    /// This class is used to collect records from <see cref="IMetricsSource"/> 
    /// and convert them to <see cref="IMetricsRecord"/>. Mutable metrics from 
    /// sources are observed and stores as immutable instances.
    /// </summary>
    internal sealed class MetricsRecordBuilder : IMetricsRecordBuilder
    {
        private readonly IList<IImmutableMetric> _metrics = new List<IImmutableMetric>();
        private readonly IList<MetricsTag> _tags = new List<MetricsTag>();
        private readonly IMetricsCollector _parentCollector;
        private readonly IMetricsInfo _info;
        private string _contextValue;
        private bool _recordBuilderFinalized;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="collector">The parent collector that creates the record builder.</param>
        /// <param name="info">Meta-data info of the record-builder.</param>
        public MetricsRecordBuilder(IMetricsCollector collector, IMetricsInfo info)
        {
            _parentCollector = collector;
            _info = info;
        }

        /// <summary>
        /// Adds a tag to the record builder.
        /// </summary>
        /// <param name="name">Name of the tag.</param>
        /// <param name="value">Value of the tag.</param>
        /// <returns>Self reference.</returns>
        public IMetricsRecordBuilder AddTag(string name, string value)
        {
            if (_recordBuilderFinalized)
            {
                throw new MetricsException("Record builder is already finalized. No more tags can be added.");
            }
            _tags.Add(new MetricsTag(new MetricsInfoImpl(name, name), value));
            return this;
        }

        /// <summary>
        /// Adds a tag to the record builder.
        /// </summary>
        /// <param name="info">Meta-data info of the tag.</param>
        /// <param name="value">Value of the tag.</param>
        /// <returns>Self reference.</returns>
        public IMetricsRecordBuilder AddTag(IMetricsInfo info, string value)
        {
            if (_recordBuilderFinalized)
            {
                throw new MetricsException("Record builder is already finalized. No more tags can be added.");
            }
            _tags.Add(new MetricsTag(info, value));
            return this;
        }

        /// <summary>
        /// Adds a tag to the record builder. This functions removes duplication in 
        /// case tag has already been created by the caller.
        /// </summary>
        /// <param name="tag">Tag to add.</param>
        /// <returns>Self reference.</returns>
        public IMetricsRecordBuilder Add(MetricsTag tag)
        {
            if (_recordBuilderFinalized)
            {
                throw new MetricsException("Record builder is already finalized. No more tags can be added.");
            }
            _tags.Add(tag);
            return this;
        }

        /// <summary>
        /// Adds a metric to the record builder. This functions removes duplication in 
        /// case metric has already been created by the caller.
        /// </summary>
        /// <param name="metric">Metric to add.</param>
        /// <returns>Self reference.</returns>
        public IMetricsRecordBuilder Add(IImmutableMetric metric)
        {
            if (_recordBuilderFinalized)
            {
                throw new MetricsException("Record builder is already finalized. No more metrics can be added.");
            }
            _metrics.Add(metric);
            return this;
        }

        /// <summary>
        /// Sets a context for the record builder. Creates the corresponding tag.
        /// </summary>
        /// <param name="value">Value of the context.</param>
        /// <returns>Self reference.</returns>
        public IMetricsRecordBuilder SetContext(string value)
        {
            if (_recordBuilderFinalized)
            {
                throw new MetricsException("Record builder is already finalized. No more metrics can be added.");
            }
            _contextValue = value;
            return AddTag(MetricsSystemConstants.Context, value);
        }

        /// <summary>
        /// Adds a counter to the record builder.
        /// </summary>
        /// <param name="info">Meta data info of the counter.</param>
        /// <param name="value">Value of the counter.</param>
        /// <returns>Self reference.</returns>
        public IMetricsRecordBuilder AddCounter(IMetricsInfo info, long value)
        {
            if (_recordBuilderFinalized)
            {
                throw new MetricsException("Record builder is already finalized. No more metrics can be added.");
            }
            _metrics.Add(new ImmutableCounter(info, value));
            return this;
        }

        /// <summary>
        /// Adds a long gauge to the record builder.
        /// </summary>
        /// <param name="info">Meta data info of the gauge.</param>
        /// <param name="value">Value of the gauge.</param>
        /// <returns>Self reference.</returns>
        public IMetricsRecordBuilder AddGauge(IMetricsInfo info, long value)
        {
            if (_recordBuilderFinalized)
            {
                throw new MetricsException("Record builder is already finalized. No more metrics can be added.");
            }
            _metrics.Add(new ImmutableLongGauge(info, value));
            return this;
        }

        /// <summary>
        /// Adds a double gauge to the record builder.
        /// </summary>
        /// <param name="info">Meta data info of the gauge.</param>
        /// <param name="value">Value of the gauge.</param>
        /// <returns>Self reference.</returns>
        public IMetricsRecordBuilder AddGauge(IMetricsInfo info, double value)
        {
            if (_recordBuilderFinalized)
            {
                throw new MetricsException("Record builder is already finalized. No more metrics can be added.");
            }
            _metrics.Add(new ImmutableDoubleGauge(info, value));
            return this;
        }

        /// <summary>
        /// Returns the parent collector.
        /// </summary>
        public IMetricsCollector ParentCollector()
        {
            return _parentCollector;
        }

        /// <summary>
        /// Ends the record building and returns the parent collector.
        /// </summary>
        /// <returns>The parent collector.</returns>
        public IMetricsCollector EndRecord()
        {
            _recordBuilderFinalized = true;
            return _parentCollector;
        }

        /// <summary>
        /// Creates the record from record builder.
        /// </summary>
        /// <returns>The newly created record. Return null if no metric or tag exists.</returns>
        public IMetricsRecord GetRecord()
        {
            TimeSpan t = DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1));
            long timeStamp = (long)t.TotalSeconds;

            return !IsEmpty() ? new MetricsRecord(_info, timeStamp, _metrics, _tags, _contextValue) : null;
        }

        /// <summary>
        /// Determines if record has any entry.
        /// </summary>
        /// <returns>True if it is empty.</returns>
        public bool IsEmpty()
        {
            return _metrics.Count == 0 && _tags.Count == 0;
        }
    }
}
