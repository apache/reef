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

namespace Org.Apache.REEF.Common.Telemetry
{
    /// <summary>
    /// Base implementation of a metric object. All metrics of a specific type should derive from this base class.
    /// </summary>
    /// <typeparam name="T">Metric type.</typeparam>
    public abstract class MetricBase<T> : IMetric<T>
    {
        protected IObserver<IMetric<T>> _tracker;

        protected T _typedValue;

        protected long _timestamp;

        protected bool _isImmutable;

        public string Name { get; }

        public string Description { get; }

        public virtual long Timestamp
        {
            get { return _timestamp; }
        }

        public virtual object ValueUntyped
        {
            get { return _typedValue; }
        }

        public virtual T Value
        {
            get
            {
                return _typedValue;
            }
        }

        public bool IsImmutable
        {
            get
            {
                return _isImmutable;
            }
        }

        public MetricBase(string name, string description, bool isImmutable)
        {
            Name = name;
            Description = description;
            _timestamp = DateTime.Now.Ticks;
            _typedValue = default(T);
            _isImmutable = isImmutable;
        }

        public MetricBase(string name, string description, long timeStamp, T value)
        {
            Name = name;
            Description = description;
            _timestamp = timeStamp;
            _typedValue = value;
        }

        public void AssignNewValue(object val)
        {
            if (val.GetType() != _typedValue.GetType())
            {
                throw new ApplicationException("Cannot assign new value to metric because of type mismatch.");
            }
            _typedValue = (T)val;
            _timestamp = DateTime.Now.Ticks;
            _tracker.OnNext(this);
        }

        public IDisposable Subscribe(IObserver<IMetric> observer)
        {
            _tracker = observer;
            return new Unsubscriber(observer);
        }

        private class Unsubscriber : IDisposable
        {
            private IObserver<IMetric> _observer;

            public Unsubscriber(IObserver<IMetric> observer)
            {
                _observer = observer;
            }

            public void Dispose()
            {
                _observer = null;
            }
        }
    }
}
