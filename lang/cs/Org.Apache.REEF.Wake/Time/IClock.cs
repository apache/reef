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
using System.Collections.Generic;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Impl;
using Org.Apache.REEF.Wake.Time.Event;
using Org.Apache.REEF.Wake.Time.Runtime.Event;

namespace Org.Apache.REEF.Wake.Time
{
    public abstract class IClock : IDisposable
    {
        /// <summary>
        /// Schedule a TimerEvent at the given future offset
        /// </summary>
        /// <param name="offset">The offset in the future to schedule the alarm</param>
        /// <param name="handler">The IObserver to to be called</param>
        public abstract void ScheduleAlarm(long offset, IObserver<Alarm> handler);

        /// <summary>
        /// Clock is idle if it has no future alarms set
        /// </summary>
        /// <returns>True if no future alarms are set, otherwise false</returns>
        public abstract bool IsIdle();

        /// <summary>
        /// Dispose of the clock and all scheduled alarms
        /// </summary>
        public abstract void Dispose();

        /// <summary>
        /// Start the Clock.
        /// </summary>
        /// <remarks>
        /// The clock will continue to run and handle events until it has been disposed.
        /// </remarks>
        public abstract void Run();

        /// <summary>
        /// Bind this to an event handler to statically subscribe to the StartTime Event
        /// </summary>
        [NamedParameter(documentation: "Will be called upon the start even", defaultClass: typeof(MissingStartHandlerHandler))]
        public class StartHandler : Name<ISet<IObserver<StartTime>>>
        {
        }

        /// <summary>
        /// Bind this to an event handler to statically subscribe to the StopTime Event
        /// </summary>
        [NamedParameter(documentation: "Will be called upon the stop event", defaultClass: typeof(LoggingEventHandler<StopTime>))]
        public class StopHandler : Name<ISet<IObserver<StopTime>>>
        {
        }

        /// <summary>
        /// Bind this to an event handler to statically subscribe to the RuntimeStart Event
        /// </summary>
        [NamedParameter(documentation: "Will be called upon the runtime start event", defaultClass: typeof(LoggingEventHandler<RuntimeStart>))]
        public class RuntimeStartHandler : Name<ISet<IObserver<RuntimeStart>>>
        {
        }

        /// <summary>
        /// Bind this to an event handler to statically subscribe to the RuntimeStop Event
        /// </summary>
        [NamedParameter(documentation: "Will be called upon the runtime stop event", defaultClass: typeof(LoggingEventHandler<RuntimeStop>))]
        public class RuntimeStopHandler : Name<ISet<IObserver<RuntimeStop>>>
        {
        }

        /// <summary>
        /// Bind this to an event handler to statically subscribe to the IdleClock Event
        /// </summary>
        [NamedParameter(documentation: "Will be called upon the Idle event", defaultClass: typeof(LoggingEventHandler<IdleClock>))]
        public class IdleHandler : Name<ISet<IObserver<IdleClock>>>
        {
        }
    }
}
