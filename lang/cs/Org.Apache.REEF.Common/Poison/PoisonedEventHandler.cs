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
using System.Reactive;
using System.Runtime.Serialization;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Time;
using Org.Apache.REEF.Wake.Time.Event;

namespace Org.Apache.REEF.Common.Poison
{
    /// <summary>
    /// Handler to process an event in a way that has certain probability of failure within certain inverval of time.
    /// </summary>
    /// <typeparam name="T">The type of event</typeparam>
    [Private]
    public class PoisonedEventHandler<T> : IObserver<T>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(PoisonedEventHandler<T>));

        private readonly double _crashProbability;
        private readonly int _crashTimeout;
        private readonly int _crashMinDelay;
        private readonly IClock _clock;

        private readonly Random _rand = new Random();

        public int TimeToCrash { get; private set; }

        [Inject]
        private PoisonedEventHandler(
            [Parameter(typeof(CrashProbability))] double crashProbability,
            [Parameter(typeof(CrashTimeout))] int crashTimeout,
            [Parameter(typeof(CrashMinDelay))] int crashMinDelay,
            IClock clock) 
        {
            _crashProbability = crashProbability;
            _crashTimeout = crashTimeout;
            _crashMinDelay = crashMinDelay;
            _clock = clock;
        }

        /// <summary>
        /// Throws a PoisonException with probability CrashProbability between time CrashMinDelay and CrashMinDelay + CrashTimeout.
        /// Uses a separate thread to throw the exception.
        /// </summary>
        public void OnNext(T value)
        {
            Logger.Log(Level.Info, "Poisoned handler for {0}", typeof(T).FullName);
            if (_rand.NextDouble() <= _crashProbability)
            {
                TimeToCrash = _rand.Next(_crashTimeout) + _crashMinDelay;
                Logger.Log(Level.Info, "Poisoning successful, crashing in {0} msec.", TimeToCrash);
                IObserver<Alarm> poisonedAlarm = Observer.Create<Alarm>(
                    x =>
                    {
                        Logger.Log(Level.Verbose, "Alarm firing");
                        throw new PoisonException("Crashed at " + DateTime.Now);
                    });
                _clock.ScheduleAlarm(TimeToCrash, poisonedAlarm);
            }
            else
            {
                TimeToCrash = -1;
                Logger.Log(Level.Info, "No poisoning happens");
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

    /// <summary>
    /// Exception thrown by PoisonedEventHandler.
    /// </summary>
    [Private]
    [Serializable]
    public class PoisonException : Exception
    {
        public PoisonException(string s) : base(s)
        {
        }

        public PoisonException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }

    [Private]
    [NamedParameter("The probability with which a crash will occur", "CrashProbability", "0.5")]
    public class CrashProbability : Name<double>
    {
    }

    [Private]
    [NamedParameter("The time window (in msec) after crash delay completes in which the crash will occur", "CrashTimeout", "1000")]
    public class CrashTimeout : Name<int>
    {
    }

    [Private]
    [NamedParameter("The time period (in msec) after event in which the crash is guaranteed to not occur", "CrashMinDelay", "0")]
    public class CrashMinDelay : Name<int>
    {
    }
}
