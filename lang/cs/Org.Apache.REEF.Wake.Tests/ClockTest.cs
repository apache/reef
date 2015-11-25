/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive;
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Wake.Time.Event;
using Org.Apache.REEF.Wake.Time.Runtime;
using Xunit;

namespace Org.Apache.REEF.Wake.Tests
{
    public class ClockTest
    {
        [Fact]
        public void TestClock()
        {
            using (RuntimeClock clock = BuildClock())
            {
                Task.Run(new Action(clock.Run));

                var heartBeat = new HeartbeatObserver(clock);
                heartBeat.OnNext(null);
                Thread.Sleep(5000);

                Assert.Equal(100, heartBeat.EventCount);
            }
        }

        [Fact]
        public void TestAlarmRegistrationRaceConditions()
        {
            using (RuntimeClock clock = BuildClock())
            {
                Task.Run(new Action(clock.Run));

                List<Alarm> events1 = new List<Alarm>();
                List<Alarm> events2 = new List<Alarm>();

                // Observers to record events that they have processed
                IObserver<Alarm> earlierRecorder = Observer.Create<Alarm>(events1.Add);
                IObserver<Alarm> laterRecorder = Observer.Create<Alarm>(events2.Add);

                // Schedule a later alarm in the future
                clock.ScheduleAlarm(5000, laterRecorder);

                // After 1 second, schedule an earlier alarm that will fire before the later alarm
                Thread.Sleep(1000);
                clock.ScheduleAlarm(2000, earlierRecorder);

                // The earlier alarm should not have fired after 1 second
                Thread.Sleep(1000);
                Assert.Equal(0, events1.Count);

                // The earlier alarm will have fired after another 1.5 seconds, but the later will have not
                Thread.Sleep(1500);
                Assert.Equal(1, events1.Count);
                Assert.Equal(0, events2.Count);

                // The later alarm will have fired after 2 seconds
                Thread.Sleep(2000);
                Assert.Equal(1, events1.Count);
            }
        }

        [Fact]
        public void TestSimultaneousAlarms()
        {
            using (RuntimeClock clock = BuildClock())
            {
                Task.Run(new Action(clock.Run));

                List<Alarm> events = new List<Alarm>();
                IObserver<Alarm> eventRecorder = Observer.Create<Alarm>(events.Add);

                clock.ScheduleAlarm(1000, eventRecorder);
                clock.ScheduleAlarm(1000, eventRecorder);
                clock.ScheduleAlarm(1000, eventRecorder);

                Thread.Sleep(1500);
                Assert.Equal(3, events.Count);
            }
        }

        [Fact]
        public void TestAlarmOrder()
        {
            using (RuntimeClock clock = BuildLogicalClock())
            {
                Task.Run(new Action(clock.Run));

                // Event handler to record event time stamps
                List<long> recordedTimestamps = new List<long>();
                IObserver<Alarm> eventRecorder = Observer.Create<Alarm>(alarm => recordedTimestamps.Add(alarm.TimeStamp));

                // Schedule 10 alarms every 100 ms 
                List<long> expectedTimestamps = Enumerable.Range(0, 10).Select(offset => (long)offset * 100).ToList();
                expectedTimestamps.ForEach(offset => clock.ScheduleAlarm(offset, eventRecorder));
    
                // Check that the recorded timestamps are in the same order that they were scheduled
                Thread.Sleep(1500);
                Assert.True(expectedTimestamps.SequenceEqual(recordedTimestamps));
            }
        }

        private RuntimeClock BuildClock()
        {
            var builder = TangFactory.GetTang().NewConfigurationBuilder();

            return TangFactory.GetTang()
                              .NewInjector(builder.Build())
                              .GetInstance<RuntimeClock>();
        }

        private RuntimeClock BuildLogicalClock()
        {
            var builder = TangFactory.GetTang().NewConfigurationBuilder();
            builder.BindImplementation(GenericType<ITimer>.Class, GenericType<LogicalTimer>.Class);

            return TangFactory.GetTang()
                              .NewInjector(builder.Build())
                              .GetInstance<RuntimeClock>();
        }

        private class HeartbeatObserver : IObserver<Alarm>
        {
            private readonly RuntimeClock _clock;

            public HeartbeatObserver(RuntimeClock clock)
            {
                _clock = clock;
                EventCount = 0;
            }

            public int EventCount { get; set; }

            public void OnNext(Alarm value)
            {
                EventCount++;
                if (EventCount < 100)
                {
                    _clock.ScheduleAlarm(10, this);
                }
            }

            public void OnError(Exception error)
            {
                throw new NotImplementedException();
            }

            public void OnCompleted()
            {
                throw new NotImplementedException();
            }
        }
    }
}
