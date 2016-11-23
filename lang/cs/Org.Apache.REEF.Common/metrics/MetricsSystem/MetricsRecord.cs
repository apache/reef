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
    /// Default implementation of <see cref="IMetricsRecord"/>. This class 
    /// maintains a collection of immutable metrics and tags.
    /// </summary>
    internal sealed class MetricsRecord : IMetricsRecord
    {
        /// <summary>
        /// Constructor for the metric record.
        /// </summary>
        /// <param name="info">Meta-data info for the record.</param>
        /// <param name="timeStamp">Unix time stamp for the record measured as total 
        /// seconds relative to Jan 01 1970. (UTC)</param>
        /// <param name="metrics">Collection of metrics in the record.</param>
        /// <param name="tags">Collection of tags in the record.</param>
        /// <param name="context">Context of the record.</param>
        public MetricsRecord(IMetricsInfo info,
            long timeStamp,
            IEnumerable<IImmutableMetric> metrics,
            IEnumerable<MetricsTag> tags,
            string context)
        {
            if (info == null)
            {
                throw new MetricsException("Record info cannot be null", new ArgumentNullException("info"));
            }

            if (timeStamp < 0)
            {
                throw new MetricsException(string.Empty,
                    new ArgumentException("Timestamp cannot be less than zero", "timeStamp"));
            }

            if (metrics == null)
            {
                throw new MetricsException("Metrics list is null", new ArgumentNullException("metrics"));
            }

            if (tags == null)
            {
                throw new MetricsException("Tag list is null", new ArgumentNullException("tags"));
            }

            Name = info.Name;
            Description = info.Description;
            Timestamp = timeStamp;
            Metrics = metrics;
            Tags = tags;
            Context = context;
        }

        /// <summary>
        /// Unix time stamp (in seconds) of the record.
        /// </summary>
        public long Timestamp { get; private set; }

        /// <summary>
        /// Name of the record.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Description of the record.
        /// </summary>
        public string Description { get; private set; }

        /// <summary>
        /// Context of the record.
        /// </summary>
        public string Context { get; private set; }

        /// <summary>
        /// Tags in the record.
        /// </summary>
        public IEnumerable<MetricsTag> Tags { get; private set; }

        /// <summary>
        /// Metrics in the record.
        /// </summary>
        public IEnumerable<IImmutableMetric> Metrics { get; private set; }
    }
}