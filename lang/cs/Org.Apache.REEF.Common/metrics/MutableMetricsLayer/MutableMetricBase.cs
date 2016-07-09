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

namespace Org.Apache.REEF.Common.Metrics.MutableMetricsLayer
{
    /// <summary>
    /// Base Mutable metrics class. Can be instanitated only from derived classes.
    /// </summary>
    internal class MutableMetricBase : IExtendedMutableMetric
    {
        private IObserver<SnapshotRequest> _observer;
        private volatile bool _changed;
        private IDisposable _unSubscriber;

        /// <summary>
        /// Protected Constructor
        /// </summary>
        /// <param name="info">Meta-data info. of the metric.</param>
        protected MutableMetricBase(IMetricsInfo info)
        {
            Info = info;
            _changed = true;
        }

        /// <summary>
        /// Registers the observer with the base class. This method will typically be used by the 
        /// metrics like Counter and Gauges in their constructor to register observer.
        /// </summary>
        /// <param name="observer">Observer to be registered.</param>
        protected void RegisterSnapshotRequestObserver(IObserver<SnapshotRequest> observer)
        {
            if (_observer != null)
            {
                throw new MetricsException("Another observer already registered with the metric");
            }
            _observer = observer;
        }

        /// <summary>
        /// Meta-data of the metric.
        /// </summary>
        public IMetricsInfo Info { get; private set; }

        /// <summary>
        /// Sets the changed flag. Called in derived classes when metric values are changed.
        /// </summary>
        public void SetChanged()
        {
            _changed = true;
        }

        /// <summary>
        /// Clears the changed flag. Called by snapshot operations after recording latest values.
        /// </summary>
        public void ClearChanged()
        {
            _changed = false;
        }

        /// <summary>
        /// True if metric value changed after taking a snapshot, false otherwise.
        /// </summary>
        public bool Changed
        {
            get { return _changed; }
        }

        /// <summary>
        /// Simply calls the OnNext() of registered observer.
        /// </summary>
        /// <param name="value">Snapshot request instance</param>
        public void OnNext(SnapshotRequest value)
        {
            _observer.OnNext(value);
        }

        /// <summary>
        /// Simply calls the OnError() of registered observer.
        /// </summary>
        /// <param name="error">Exception message</param>
        public void OnError(Exception error)
        {
            _observer.OnError(error);
        }

        /// <summary>
        /// Simply calls OnCompleted() of registered observer.
        /// </summary>
        public void OnCompleted()
        {
            _observer.OnCompleted();
        }

        /// <summary>
        /// Used to subscribe itself with the provider, typically an <see cref="IMetricsSource"/>.
        /// </summary>
        /// <param name="requestor">Provider.</param>
        public void Subscribe(IObservable<SnapshotRequest> requestor)
        {
            if (requestor != null)
            {
                _unSubscriber = requestor.Subscribe(this);
            }
        }

        /// <summary>
        /// Used to un-subscribe itself with the provider, typically an <see cref="IMetricsSource"/>.
        /// Useful once metrics has outlived it's utility and there is no need to transmit it.
        /// </summary>
        public void UnSubscribe()
        {
            _unSubscriber.Dispose();
        }
    }
}
