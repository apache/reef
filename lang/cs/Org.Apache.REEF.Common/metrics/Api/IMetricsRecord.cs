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

using System.Collections.Generic;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Metrics.Api
{
    /// <summary>
    /// Immutable record of the metric. Represents snapshot of set of metrics
    /// with a timestamp.
    /// </summary>
    [Unstable("0.16", "Contract may change.")]
    public interface IMetricsRecord
    {
        /// <summary>
        /// Gets the timestamp of the metric in seconds.
        /// Exact semantics is left to the default implementation. 
        /// One option is to create the seconds-since-the-UNIX-epoch mentioned at this page.
        /// https://blogs.msdn.microsoft.com/brada/2004/03/20/seconds-since-the-unix-epoch-in-c/ 
        /// </summary>
        long Timestamp { get; }

        /// <summary>
        /// Name of the record.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Description of the record.
        /// </summary>
        string Description { get; }

        /// <summary>
        /// Context name of the record.
        /// </summary>
        string Context { get; }

        /// <summary>
        /// Get the tags of the record.
        /// </summary>
        IEnumerable<MetricsTag> Tags { get; }

        /// <summary>
        /// Get the metrics of the record.
        /// </summary>
        IEnumerable<IImmutableMetric> Metrics { get; }
    }
}
