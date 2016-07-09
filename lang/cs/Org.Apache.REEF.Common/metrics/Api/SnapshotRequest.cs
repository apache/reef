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

using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Metrics.Api
{
    /// <summary>
    /// Class representing the Snapshot request.
    /// </summary>
    [Unstable("0.16", "Contract may change.")]
    public sealed class SnapshotRequest
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="builder">Metrics record builder to be used to add metrics to record.</param>
        /// <param name="fullSnapshot">If true, record even uncanged metrics.</param>
        internal SnapshotRequest(IMetricsRecordBuilder builder, bool fullSnapshot)
        {
            Builder = builder;
            FullSnapshot = fullSnapshot;
        }

        /// <summary>
        /// Constructor. Sets <see cref="FullSnapshot"/> to false.
        /// </summary>
        /// <param name="builder">Metrics record builder to be used to add metrics to record.</param>
        internal SnapshotRequest(IMetricsRecordBuilder builder)
        {
            Builder = builder;
            FullSnapshot = false;
        }

        /// <summary>
        /// Builder to add metrics to the record.
        /// </summary>
        public IMetricsRecordBuilder Builder { get; private set; }

        /// <summary>
        /// Determines whether to take snapshot of unchangesmetrics (true) or not (false).
        /// </summary>
        public bool FullSnapshot { get; private set; }
    }
}
