﻿// Licensed to the Apache Software Foundation (ASF) under one
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
// under the License

using System;
using System.Threading;
using Xunit;
using Org.Apache.REEF.Wake;
using Org.Apache.REEF.Wake.Impl;
using Org.Apache.REEF.Wake.Tests;

namespace Org.Apache.REEF.Wake.Tests
{
    // Timer stage tests.
    public class TimerStageTest
    {
        private static long _delay = 100;
        private static long _period = 1000;
        private static long _bigPeriod = Int64.MaxValue;

        [Fact]
        public void testValidTimerPeriod()
        {
            runTest(_delay, _period);
        }

        [Fact]
        public void testInvalidTimerPeriod()
        {
            Assert.Throws<ArgumentException>(() => runTest(_delay, _bigPeriod));
        }

        [Fact]
        public void testInvalidTimerDelay()
        {
            Assert.Throws<ArgumentException>(() => runTest(_bigPeriod, _period));
        }

        void runTest(long delay, long period)
        {
            TimerMonitor monitor = new TimerMonitor();
            int expected = 10;

            TestEventHandler handler = new TestEventHandler(monitor, expected);
            IStage stage = new TimerStage(handler, delay, period);

            monitor.Mwait();
            Assert.Equal(expected, handler.getCount());
        }

        public class TestEventHandler : IEventHandler<PeriodicEvent>
        {
            private TimerMonitor _monitor;
            private long _expected;
            private long _count;

            public TestEventHandler(TimerMonitor monitor, long expected)
            {
                _count = 0;
                _monitor = monitor;
                _expected = expected;
            }

            public void OnNext(PeriodicEvent e)
            {
                long count = Interlocked.Increment(ref _count);
                if (Interlocked.Read(ref _count) == _expected)
                {
                    _monitor.Mnotify();
                }
            }

            public long getCount()
            {
                return Interlocked.Read(ref _count);
            }
        }

        public class TimerMonitor
        {
            private long finished;

            public TimerMonitor()
            {
                finished = 0;
            }

            public void Mwait()
            {
                lock (this)
                {
                    while (Interlocked.Read(ref this.finished) < 1)
                    {
                        Monitor.Wait(this);
                    }
                    Interlocked.CompareExchange(ref finished, 0, 1);
                }
            }

            public void Mnotify()
            {
                lock (this)
                {
                    Interlocked.CompareExchange(ref finished, 1, 0);
                    Monitor.Pulse(this);
                }
            }
        }
    }
}