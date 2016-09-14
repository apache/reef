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

namespace Org.Apache.REEF.Wake.Impl
{
    /// <summary>Stage that triggers an event handler periodically</summary>
    public sealed class TimerStage : IStage
    {
        // private readonly ScheduledExecutorService executor;
        private readonly Timer _timer;
        private readonly PeriodicEvent _value = new PeriodicEvent();
        private readonly IEventHandler<PeriodicEvent> _handler;

        /// <summary>Constructs a timer stage with no initial delay</summary>
        /// <param name="handler">an event handler</param>
        /// <param name="period">a period in milli-seconds</param>
        public TimerStage(IEventHandler<PeriodicEvent> handler, long period) : this(handler, 0, period)
        {
        }

        /// <summary>Constructs a timer stage</summary>
        /// <param name="handler">an event handler</param>
        /// <param name="initialDelay">an initial delay</param>d
        /// <param name="period">a period in milli-seconds</param>
        public TimerStage(IEventHandler<PeriodicEvent> handler, long initialDelay, long period)
        {
            // Core .NET only supports 32 bit timers.
            validate("initialDelay", initialDelay);
            validate("initialDelay", period);

            _handler = handler;
            _timer = new System.Threading.Timer(
                (object state) => { OnTimedEvent(_handler, _value); }, this, (int)initialDelay, (int)period);
        }

        /// <summary>
        /// Closes resources
        /// </summary>
        public void Dispose()
        {
            _timer.Dispose();
        }

        private static void OnTimedEvent(IEventHandler<PeriodicEvent> handler, PeriodicEvent value)
        {
            handler.OnNext(value);
        }

        /// <summary>
        /// Validates the input is less than Int32.MaxInt. 
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="value">Parameter value</param>
        /// <exception cref="ArgumentException">Input value excess Int32.Max</exception>
        private void validate(string name, long value)
        {
            if (value > int.MaxValue)
            {
                throw new ArgumentException(string.Format(
                    "Parameter: " + name + " {0} is larger supported value {0}", int.MaxValue));
            }
        }
    }
}