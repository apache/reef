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
    /// Provides a base implementation. Value of metric should be provided in derived classes. 
    /// </summary>
    public abstract class MetricBase : IMetric
    {
        protected ITracker _tracker;

        protected object _metricLock = new object();

        public string Name
        {
            get; internal set;
        }

        public string Description
        {
            get; internal set;
        }

        public bool KeepUpdateHistory
        {
            get; internal set;
        }

        public abstract object ValueUntyped
        {
            get;
        }

        public MetricBase()
        {
        }

        public MetricBase(string name, string description, bool keepUpdateHistory = true)
        {
            Name = name;
            Description = description;
            KeepUpdateHistory = keepUpdateHistory;
        }

        [JsonConstructor]
        public MetricBase(string name, string description, object valueUntyped, bool keepUpdateHistory)
        {
            Name = name;
            Description = description;
            KeepUpdateHistory = keepUpdateHistory;
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

    /// <summary>
    /// Base implementation with a generic value type.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class MetricBase<T> : MetricBase
    {
        protected T _typedValue;

        public override object ValueUntyped
        {
            get { return _typedValue; }
        }

        public T Value
        {
            get { return _typedValue; }
        }

        public MetricBase() : base()
        {
            _typedValue = default;
        }

        public MetricBase(string name, string description, bool keepUpdateHistory = true)
            : base(name, description, keepUpdateHistory)
        {
            _typedValue = default;
        }

        [JsonConstructor]
        public MetricBase(string name, string description, T value, bool keepUpdateHistory)
            : base(name, description, value, keepUpdateHistory)
        {
            _typedValue = value;
        }

        /// <summary>
        /// Assign and track the new value to metric. 
        /// In most cases, this method should be overridden in derived classes using Interlocked.
        /// </summary>
        /// <param name="value">Value to assign the metric.</param>
        public abstract void AssignNewValue(T value);
    }
}
