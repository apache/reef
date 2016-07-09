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
using Org.Apache.REEF.Common.Metrics.MutableMetricsLayer;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Metrics.Api
{
    /// <summary>
    /// Metrics source interface. The current snapshot of metrics is taken via this 
    /// interface which are then later on pushed to sink. Derived from <see cref="IObservable{IMetricsRecordBuilder}"/>. 
    /// <see cref="GetMetrics"/> will call OnNext() of different mutable metrics that implement 
    /// <see cref="IObserver{IMetricsRecordBuilder}"/>
    /// </summary>
    [DefaultImplementation(typeof(DefaultMetricsSourceImpl))]
    [Unstable("0.16", "Contract may change.")]
    public interface IMetricsSource : IObservable<SnapshotRequest>
    {
        /// <summary>
        /// Gets metrics from the source.
        /// </summary>
        /// <param name="collector">Collector that stores the resulting metrics snapshot as records.</param>
        /// <param name="all">If true, gets metric values even if they are unchanged.</param>
        void GetMetrics(IMetricsCollector collector, bool all);
    }
}