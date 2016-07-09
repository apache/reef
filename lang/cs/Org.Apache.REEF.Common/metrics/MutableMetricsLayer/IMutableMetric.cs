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
using Org.Apache.REEF.Common.Metrics.Api;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Metrics.MutableMetricsLayer
{
    /// <summary>
    /// Base mutable metric interface from which all other metrics derive.
    /// </summary>
    [Unstable("0.16", "Contract may change.")]
    public interface IMutableMetric : IObserver<SnapshotRequest>
    {
        /// <summary>
        /// Meta-data of the metric.
        /// </summary>
        IMetricsInfo Info { get; }

        /// <summary>
        /// This function should be used to subscribe metric tot he observable, 
        /// typically <see cref="IMetricsSource"/>
        /// </summary>
        void Subscribe(IObservable<SnapshotRequest> requestor);
    }
}
