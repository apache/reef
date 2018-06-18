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
using System.Threading;
using Newtonsoft.Json;

namespace Org.Apache.REEF.Common.Telemetry
{
    /// <summary>
    /// Base implementation of a metric object.
    /// </summary>
    /// <typeparam name="T">Metric type</typeparam>
    public class MetricBase<T> : IMetric<T>
    {
        protected ITracker _tracker;

        internal T _typedValue;

        internal bool _keepUpdateHistory;

        private object _metricLock = new object();

        public string Name { get; internal set; }

        public string Description { get; internal set; }

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

        public bool KeepUpdateHistory
        {
            get
            {
                return _keepUpdateHistory;
            }
        }

        public MetricBase()
        {
        }

        public MetricBase(string name, string description, bool keepUpdateHistory = true)
        {
            Name = name;
            Description = description;
            _typedValue = default(T);
            _keepUpdateHistory = keepUpdateHistory;
        }

        [JsonConstructor]
        public MetricBase(string name, string description, T value, bool keepUpdateHistory)
        {
            Name = name;
            Description = description;
            _typedValue = value;
            _keepUpdateHistory = keepUpdateHistory;
        }

        public virtual void AssignNewValue(object val)
        {
            ValidateValueType(val);

            lock (_metricLock)
            {
                _typedValue = (T)val;
            }
            _tracker.Track(val);
        }

        protected void ValidateValueType(object val)
        {
            if (val.GetType() != _typedValue.GetType())
            {
                throw new ApplicationException("Cannot assign new value to metric because of type mismatch.");
            }
        }

        public IDisposable Subscribe(ITracker observer)
        {
            _tracker = observer;
            return new Unsubscriber(observer);
        }

        private class Unsubscriber : IDisposable
        {
            private ITracker _tracker;

            public Unsubscriber(ITracker tracker)
            {
                _tracker = tracker;
            }

            public void Dispose()
            {
                _tracker = null;
            }
        }
    }
}
