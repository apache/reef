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
// under the License

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

using Org.Apache.REEF.Wake;
using Org.Apache.REEF.Wake.Impl;
using Org.Apache.REEF.Wake.Tests;

using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Time;
using Org.Apache.REEF.Wake.Time.Event;
using Org.Apache.REEF.Wake.Time.Runtime;
using Xunit;

namespace Org.Apache.REEF.Wake.Tests
{
    // Timer stage tests.
    public class TimerStageTest
    {
        private static long _shutdownTimeout = 1000;

        [Fact]
        public void testTimerStage()
        {
            TimerMonitor monitor = new TimerMonitor();
            int expected = 10;

            TestEventHandler handler = new TestEventHandler(monitor, expected);
            IStage stage = new TimerStage(handler, 100, _shutdownTimeout);

            monitor.Mwait();

            // stage.Close();
            Assert.Equal(expected, handler.getCount());
        }

        public class TestEventHandler : IEventHandler<PeriodicEvent>
        {
            private TimerMonitor _monitor;
            private long _expected;
            private long _count;
            private Stopwatch _stopwatch = new Stopwatch();

            public TestEventHandler(TimerMonitor monitor, long expected)
            {
                _count = 0;
                _monitor = monitor;
                _expected = expected;
                _stopwatch.Start();
            }

            public void OnNext(PeriodicEvent e)
            {
                long count = Interlocked.Increment(ref _count);
                System.Console.WriteLine(count + " " + e + " scheduled event at " + _stopwatch.ElapsedMilliseconds);
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