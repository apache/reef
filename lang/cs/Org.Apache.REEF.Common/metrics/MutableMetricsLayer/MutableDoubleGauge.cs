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
    /// Mutable double gauge metrics class.
    /// </summary>
    internal sealed class MutableDoubleGauge : MutableMetricBase, IDoubleGauge
    {
        private double _value = 0;
        private readonly object _lock = new object();
        private static readonly Logger Logger = Logger.GetLogger(typeof(MutableDoubleGauge));

        /// <summary>
        /// Gauge Constructor
        /// </summary>
        /// <param name="info">Meta-data info. of the metric.</param>
        /// <param name="initValue">Initial gauge value.</param>
        public MutableDoubleGauge(IMetricsInfo info, double initValue)
            : base(info)
        {
            RegisterSnapshotRequestObserver(Observer.Create<SnapshotRequest>(this.GiveSnapshot,
                this.SnapshotError,
                UnSubscribe));
            _value = initValue;
        }

        /// <summary>
        /// Gauge Constructor. Initializes gauge to zero.
        /// </summary>
        /// <param name="info">Meta-data info. of the metric.</param>
        public MutableDoubleGauge(IMetricsInfo info)
            : this(info, 0)
        {
        }

        /// <summary>
        /// Gauge Constructor. Initializes gauge to zero.
        /// </summary>
        /// <param name="name">Name of the gauge.</param>
        public MutableDoubleGauge(string name)
            : this(new MetricsInfoImpl(name, name), 0)
        {
        }

        /// <summary>
        /// Decrements the gauge by 1.
        /// </summary>
        public void Decrement()
        {
            Decrement(1);
        }

        /// <summary>
        /// Decrements the gauge by delta.
        /// </summary>
        /// <param name="delta">Value with which to decrement.</param>
        public void Decrement(double delta)
        {
            lock (_lock)
            {
                _value -= delta;
                SetChanged();
            }
        }

        /// <summary>
        /// Resets the gauge.
        /// </summary>
        /// <param name="value">Value to which the gauge should be reset.</param>
        public void Reset(double value)
        {
            lock (_lock)
            {
                _value = value;
            }
        }

        /// <summary>
        /// Increments the gauge by 1.
        /// </summary>
        public void Increment()
        {
            Increment(1);
        }

        /// <summary>
        /// Increments the gauge by delta.
        /// </summary>
        /// <param name="delta">Value with which to increment.</param>
        public void Increment(double delta)
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
                    request.Builder.AddGauge(Info, _value);
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
