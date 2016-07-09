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
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Metrics.Api
{
    /// <summary>
    /// Metrics filter interface used to filter metrics at different levels - 
    /// source and sink names, record, individual tag, individual metric.
    /// </summary>
    [Unstable("0.16", "Contract may change.")]
    public interface IMetricsFilter
    {
        /// <summary>
        /// Returns a function indicating whether to accept the name string
        /// (can be from metric, source, sink, record etc.). Returns True if accepted, 
        /// false otherwise.
        /// </summary>
        Func<string, bool> AcceptsName { get; }

        /// <summary>
        /// Returns a function indicating whether to accept the tag. Returns True if accepted, 
        /// false otherwise.
        /// </summary>
        Func<MetricsTag, bool> AcceptsTag { get; }

        /// <summary>
        /// Returns a function indicating whether to accept the record. Returns True if accepted, 
        /// false otherwise.
        /// </summary>
        Func<IMetricsRecord, bool> AcceptsRecord { get; }
    }
}
