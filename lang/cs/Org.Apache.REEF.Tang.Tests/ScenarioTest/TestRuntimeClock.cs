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
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Xunit;

namespace Org.Apache.REEF.Tang.Tests.ScenarioTest
{
    [DefaultImplementation(typeof(RealTimer))]
    public interface ITimer
    {
        int GetCurrent();

        int GetDuration(int time);

        bool IsReady(int time);
    }

    [DefaultImplementation(typeof(RuntimeClock))]
    public interface IClock
    {
        DateTime CurrentTime();
    }

    public interface IEventHandler<T>
    {
        void OnNext(T value);
    }

    public class TestScenarios
    {
        [Fact]
        public void TestRuntimeClock()
        {
            var r = (RuntimeClock)TangFactory.GetTang().NewInjector().GetInstance(typeof(RuntimeClock));
            Assert.NotNull(r);
            r.CurrentTime();
        }

        [Fact]
        public void TestEvaluatorRuntime()
        {
            ConfigurationModule module =
                new ConfigurationModuleBuilder()
                .BindSetEntry<RuntimeStartHandler, EvaluatorRuntime, IObserver<RuntimeStart>>(GenericType<RuntimeStartHandler>.Class, GenericType<EvaluatorRuntime>.Class)
                .Build();
            IConfiguration clockConfiguration = module.Build();

            RuntimeClock clock = TangFactory.GetTang().NewInjector(clockConfiguration).GetInstance<RuntimeClock>();
            var r = clock.ClockRuntimeStartHandler.Get();
            Assert.Equal(r.Count, 1);
            foreach (var e in r)
            {
                Assert.True(e is EvaluatorRuntime);
            }
        }
    }

    public class RuntimeClock : IClock
    {      
        [Inject]
        public RuntimeClock(ITimer timer,
                            [Parameter(typeof(StartHandler))] IInjectionFuture<ISet<IEventHandler<StartTime>>> startHandler,
                            [Parameter(typeof(RuntimeStartHandler))] IInjectionFuture<ISet<IObserver<RuntimeStart>>> runtimeStartHandler,
                            [Parameter(typeof(RuntimeStopHandler))] IInjectionFuture<ISet<IObserver<RuntimeStop>>> runtimeStopHandler)
        {
            this.ClockStartHandler = startHandler;
            this.ClockRuntimeStartHandler = runtimeStartHandler;
            this.ClockRuntimeStopHandler = runtimeStopHandler;
        }

        public IInjectionFuture<ISet<IObserver<RuntimeStart>>> ClockRuntimeStartHandler { get; set; }

        public IInjectionFuture<ISet<IObserver<RuntimeStop>>> ClockRuntimeStopHandler { get; set; }

        public IInjectionFuture<ISet<IEventHandler<StartTime>>> ClockStartHandler { get; set; }

        public DateTime CurrentTime()
        {
            return DateTime.Now;
        }
    }

    [NamedParameter(DefaultClass = typeof(MissingStartHandlerHandler),
        Documentation = "Will be called upon the start event")]
    public class StartHandler : Name<ISet<IEventHandler<StartTime>>>
    {
    }

    /// <summary>
    /// Bind this to an event handler to statically subscribe to the RuntimeStart Event
    /// </summary>
    [NamedParameter(Documentation = "Will be called upon the runtime start event",
        DefaultClass = typeof(LoggingEventHandler<RuntimeStart>))]
    public class RuntimeStartHandler : Name<ISet<IObserver<RuntimeStart>>>
    {
    }

    [NamedParameter(documentation: "Will be called upon the runtime start event",
    defaultClass: typeof(LoggingEventHandler<RuntimeStop>))]
    public class RuntimeStopHandler : Name<ISet<IObserver<RuntimeStop>>>
    {
    }

    public class StartTime : Time
    {
        public StartTime(long timestamp) : base(timestamp)
        {
        }
    }

    public class RuntimeStart : Time
    {
        public RuntimeStart(long timeStamp)
            : base(timeStamp)
        {
        }
    }

    public class RuntimeStop : Time
    {
        public RuntimeStop(long timeStamp)
            : base(timeStamp)
        {
        }
    }

    public class EvaluatorRuntime : IObserver<RuntimeStart>
    {
        [Inject]
        public EvaluatorRuntime()
        {            
        }

        public void OnCompleted()
        {
            throw new NotImplementedException();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnNext(RuntimeStart value)
        {
            throw new NotImplementedException();
        }
    }

    public class LoggingEventHandler<T> : IObserver<T>
    {
        [Inject]
        public LoggingEventHandler()
        {
        }

        /// <summary>Logs the event</summary>
        /// <param name="value">an event</param>
        public void OnNext(T value)
        {
            throw new NotImplementedException();
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

    public abstract class Time : IComparable<Time>
    {
        private readonly long timestamp;

        public Time(long timestamp)
        {
            this.timestamp = timestamp;
        }

        public long GetTimeStamp()
        {
            return this.timestamp;
        }

        public int CompareTo(Time other)
        {
            throw new NotImplementedException();
        }
    }

    public class MissingStartHandlerHandler : IEventHandler<StartTime> 
    {
      [Inject]
      public MissingStartHandlerHandler() 
      {
      }

      public void OnNext(StartTime value) 
      {
      }
    }

    public class RealTimer : ITimer 
    {
        [Inject]
        public RealTimer() 
        {
        }

        public int GetCurrent() 
        {
            return DateTime.Now.Millisecond;
        }

        public int GetDuration(int time) 
        {
            return time - GetCurrent();
        }

        public bool IsReady(int time) 
        {
            return GetDuration(time) <= 0;
        }
    }
}