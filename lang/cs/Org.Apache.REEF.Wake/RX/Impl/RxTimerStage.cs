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
using System.Timers;
using Org.Apache.REEF.Wake.Impl;

namespace Org.Apache.REEF.Wake.RX.Impl
{
    /// <summary>Timer stage that provides events to the observer periodically</summary>
    public sealed class RxTimerStage : IStage, IStaticObservable
    {
        private readonly Timer _timer;
        private readonly PeriodicEvent _value = new PeriodicEvent();
        private readonly IObserver<PeriodicEvent> _observer;

        /// <summary>Constructs a Rx timer stage</summary>
        /// <param name="observer">the observer</param>
        /// <param name="period">the period in milli-seconds</param>
        public RxTimerStage(IObserver<PeriodicEvent> observer, long period) 
            : this(observer, 0, period)
        {
        }

        /// <summary>Constructs a Rx timer stage</summary>
        /// <param name="observer">the observer</param>
        /// <param name="initialDelay">the initial delay in milli-seconds</param>
        /// <param name="period">the period in milli-seconds</param>
        public RxTimerStage(IObserver<PeriodicEvent> observer, long initialDelay, long period)
        {
            _observer = observer;
            _timer = new Timer(period);
            _timer.Elapsed += (sender, e) => OnTimedEvent(sender, e, _observer, _value);
            _timer.Enabled = true;
        }

        /// <summary>
        /// Closes the stage
        /// </summary>
        public void Dispose()
        {
            _timer.Stop();
        }

        private static void OnTimedEvent(object source, ElapsedEventArgs e, IObserver<PeriodicEvent> observer, PeriodicEvent value)
        {
            observer.OnNext(value);
        }
    }
}
