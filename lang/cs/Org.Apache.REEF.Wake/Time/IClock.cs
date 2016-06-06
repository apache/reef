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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Time.Event;
using Org.Apache.REEF.Wake.Time.Runtime;

namespace Org.Apache.REEF.Wake.Time
{
    [DefaultImplementation(typeof(RuntimeClock))]
    public interface IClock : IDisposable
    {
        /// <summary>
        /// Schedule a TimerEvent at the given future offset
        /// </summary>
        /// <param name="offset">The offset in the future to schedule the alarm</param>
        /// <param name="handler">The IObserver to to be called</param>
        void ScheduleAlarm(long offset, IObserver<Alarm> handler);

        /// <summary>
        /// Clock is idle if it has no future alarms set
        /// </summary>
        /// <returns>True if no future alarms are set, otherwise false</returns>
        bool IsIdle();

        /// <summary>
        /// Start the Clock.
        /// </summary>
        /// <remarks>
        /// The clock will continue to run and handle events until it has been disposed.
        /// </remarks>
        void Run();
    }
}
