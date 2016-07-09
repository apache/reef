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
using System.Reactive;
using Org.Apache.REEF.Common.Metrics.Api;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Metrics.MutableMetricsLayer
{
    /// <summary>
    /// Mutable Counter metric class.
    /// </summary>
    internal sealed class MutableCounter : MutableMetricBase, ICounter
    {
        private long _value = 0;
        private readonly object _lock = new object();
        private static readonly Logger Logger = Logger.GetLogger(typeof(MutableCounter));

        /// <summary>
        /// Counter Constructor
        /// </summary>
        /// <param name="info">Meta-data info. of the metric.</param>
        /// <param name="initValue">Initial counter value.</param>
        public MutableCounter(IMetricsInfo info, long initValue)
            : base(info)
        {
            RegisterSnapshotRequestObserver(Observer.Create<SnapshotRequest>(this.GiveSnapshot,
                this.SnapshotError,
                UnSubscribe));
            _value = initValue;
        }

        /// <summary>
        /// Counter Constructor. Initializes counter to zero.
        /// </summary>
        /// <param name="info">Meta-data info. of the metric.</param>
        public MutableCounter(IMetricsInfo info) : this(info, 0)
        {
        }

        /// <summary>
        /// Counter Constructor. Initializes counter to zero.
        /// </summary>
        /// <param name="name">Name of the counter.</param>
        public MutableCounter(string name)
            : this(new MetricsInfoImpl(name, name), 0)
        {
        }

        /// <summary>
        /// Increments the counter by 1.
        /// </summary>
        public void Increment()
        {
            Increment(1);
        }

        /// <summary>
        /// Increments the counter by delta.
        /// </summary>
        /// <param name="delta">Value with which to increment.</param>
        public void Increment(long delta)
        {
            lock (_lock)
            {
                _value += delta;
                SetChanged();
            }
        }

        private void GiveSnapshot(SnapshotRequest request)
        {
            lock (_lock)
            {
                if (request.FullSnapshot || Changed)
                {
                    request.Builder.AddCounter(Info, _value);
                    ClearChanged();
                }
            }
        }

        private void SnapshotError(Exception e)
        {
            Logger.Log(Level.Error, "Exception happened while trying to take the snapshot");
        }
    }
}
