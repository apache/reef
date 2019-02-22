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
// under the License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Exceptions;
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Collections;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.RX.Impl;
using Org.Apache.REEF.Wake.Time.Event;
using Org.Apache.REEF.Wake.Time.Runtime.Event;
using Org.Apache.REEF.Wake.Time;
using Org.Apache.REEF.Wake.Time.Runtime;

namespace Org.Apache.REEF.Network.Elastic.Failures
{
    /// <summary>
    /// Clock used to trigger failures events.
    /// </summary>
    internal sealed class FailuresClock : IClock
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(FailuresClock));

        private static int numberOfInstantiations = 0;

        private readonly ITimer _timer;
        private readonly PubSubSubject<Time> _handlers = new PubSubSubject<Time>();
        private readonly PriorityQueue<Time> _schedule = new PriorityQueue<Time>();

        private readonly IInjectionFuture<ISet<IObserver<StartTime>>> _startHandler;
        private readonly IInjectionFuture<ISet<IObserver<StopTime>>> _stopHandler;
        private readonly IInjectionFuture<ISet<IObserver<RuntimeStart>>> _runtimeStartHandler;
        private readonly IInjectionFuture<ISet<IObserver<RuntimeStop>>> _runtimeStopHandler;
        private readonly IInjectionFuture<ISet<IObserver<IdleClock>>> _idleHandler;

        private volatile bool _disposed;

        /// <summary>
        /// Create a new failure clock with injectable IObservers.
        /// </summary>
        /// <param name="timer">The runtime clock timer</param>
        /// <param name="startHandler">The start handler</param>
        /// <param name="stopHandler">The stop handler</param>
        /// <param name="runtimeStartHandler">The runtime start handler</param>
        /// <param name="runtimeStopHandler">The runtime stop handler</param>
        /// <param name="idleHandler">The idle handler</param>
        [Inject]
        private FailuresClock(
            ITimer timer,
            [Parameter(typeof(Wake.Time.Parameters.StartHandler))] IInjectionFuture<ISet<IObserver<StartTime>>> startHandler,
            [Parameter(typeof(Wake.Time.Parameters.StopHandler))] IInjectionFuture<ISet<IObserver<StopTime>>> stopHandler,
            [Parameter(typeof(Wake.Time.Parameters.RuntimeStartHandler))] IInjectionFuture<ISet<IObserver<RuntimeStart>>> runtimeStartHandler,
            [Parameter(typeof(Wake.Time.Parameters.RuntimeStopHandler))] IInjectionFuture<ISet<IObserver<RuntimeStop>>> runtimeStopHandler,
            [Parameter(typeof(Wake.Time.Parameters.IdleHandler))] IInjectionFuture<ISet<IObserver<IdleClock>>> idleHandler)
        {
            _timer = timer;
            _startHandler = startHandler;
            _stopHandler = stopHandler;
            _runtimeStartHandler = runtimeStartHandler;
            _runtimeStopHandler = runtimeStopHandler;
            _idleHandler = idleHandler;

            ++numberOfInstantiations;
            if (numberOfInstantiations > 1)
            {
                Log.Log(Level.Warning, "Instantiated `RuntimeClock` instance number {0}", numberOfInstantiations);
            }
        }

        /// <summary>
        /// Schedule an alarm at the given future offset.
        /// </summary>
        /// <param name="offset">The offset in the future to schedule the alarm, in msec</param>
        /// <param name="handler">The IObserver to to be called</param>
        public void ScheduleAlarm(long offset, IObserver<Alarm> handler)
        {
            if (_disposed)
            {
                return;
            }
            if (handler == null)
            {
                throw new ArgumentNullException("handler");
            }

            lock (_schedule)
            {
                _schedule.Add(new ClientAlarm(_timer.CurrentTime + offset, handler));
                Monitor.PulseAll(_schedule);
            }
        }

        /// <summary>
        /// Schedule a timeout at the given future offset.
        /// </summary>
        /// <param name="offset">The offset in the future to schedule the alarm, in msec</param>
        /// <param name="handler">The IObserver to be called</param>
        public void ScheduleAlarm(ITimeout offset)
        {
            if (_disposed)
            {
                return;
            }

            lock (_schedule)
            {
                _schedule.Add(offset.GetAlarm(_timer.CurrentTime));
                Monitor.PulseAll(_schedule);
            }
        }

        /// <summary>
        /// Clock is idle if it has no future alarms set.
        /// </summary>
        /// <returns>True if no future alarms are set, otherwise false</returns>
        public bool IsIdle()
        {
            lock (_schedule)
            {
                return _schedule.Count == 0;
            }
        }

        /// <summary>
        /// Dispose of the clock and all scheduled alarms.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            lock (_schedule)
            {
                _schedule.Clear();
                _schedule.Add(new StopTime(_timer.CurrentTime));
                Monitor.PulseAll(_schedule);
            }
        }

        /// <summary>
        /// Start the clock.
        /// Clock will continue to run and handle events until it has been disposed.
        /// </summary>
        public void Run()
        {
            SubscribeHandlers();

            Exception runtimeException = null;
            try
            {
                _handlers.OnNext(new RuntimeStart(_timer.CurrentTime));
                _handlers.OnNext(new StartTime(_timer.CurrentTime));

                Time alarm = null;

                while (!(alarm is StopTime))
                {
                    lock (_schedule)
                    {
                        if (IsIdle())
                        {
                            _handlers.OnNext(new IdleClock(_timer.CurrentTime));
                        }

                        // Blocks and releases lock until it receives the next event
                        alarm = GetNextEvent();
                        ProcessEvent(alarm);
                    }
                }
            }
            catch (Exception e)
            {
                runtimeException = new ReefRuntimeException("Caught Exception in clock, failing the Evaluator.", e);
            }

            _handlers.OnNext(new RuntimeStop(_timer.CurrentTime, runtimeException));
        }

        /// <summary>
        /// Register the event handlers
        /// </summary>
        private void SubscribeHandlers()
        {
            Subscribe(_startHandler.Get());
            Subscribe(_stopHandler.Get());
            Subscribe(_runtimeStartHandler.Get());
            Subscribe(_runtimeStopHandler.Get());
            Subscribe(_idleHandler.Get());
        }

        /// <summary>
        /// Subscribe a set of IObservers for a particular Time event
        /// </summary>
        /// <param name="observers">The set of observers to subscribe</param>
        private void Subscribe<U>(ISet<IObserver<U>> observers) where U : Time
        {
            foreach (IObserver<U> observer in observers)
            {
                _handlers.Subscribe(observer);
            }
        }

        /// <summary>
        /// Wait until the first scheduled alarm is ready to be handled
        /// Assumes that we have a lock on the _schedule SortedSet
        /// </summary>
        private Time GetNextEvent()
        {
            // Wait for an alarm to be scheduled on the condition variable Count
            while (_schedule.Count == 0)
            {
                Monitor.Wait(_schedule);
            }

            // Once the alarm is scheduled, wait for the prescribed amount of time.
            // If a new alarm is scheduled with a shorter duration, Wait will preempt
            // and duration will update to reflect the new alarm's timestamp
            for (long duration = _timer.GetDuration(_schedule.First().TimeStamp);
                 duration > 0;
                 duration = _timer.GetDuration(_schedule.First().TimeStamp))
            {
                Monitor.Wait(_schedule, TimeSpan.FromMilliseconds(duration));
            }

            return _schedule.Dequeue();
        }

        /// <summary>
        /// Process the next Time event.
        /// </summary>
        /// <param name="time">The Time event to handle</param>
        private void ProcessEvent(Time time)
        {
            if (time is Alarm alarm)
            {
                alarm.Handle();
            }
            else
            {
                _handlers.OnNext(time);
            }
        }
    }
}