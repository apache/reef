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

using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Wake.Time;
using Org.Apache.REEF.Wake.Time.Event;
using Org.Apache.REEF.Wake.Time.Runtime.Event;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Bridge.Core.Common.Driver
{
    internal sealed class BridgeClock : IClock
    {
        internal struct AlarmInfo
        {
            public string AlarmId { get; set; }

            public long Timestamp { get; set; }
        }

        private static readonly Logger Log = Logger.GetLogger(typeof(BridgeClock));

        private readonly IInjectionFuture<IDriverServiceClient> _driverServiceClient;

        private readonly IInjectionFuture<IDriverClientService> _driverClientService;

        private readonly IDictionary<string, IObserver<Alarm>> _alarmDictionary = new Dictionary<string, IObserver<Alarm>>();

        private int _outstandingAlarms;

        [Inject]
        private BridgeClock(
            IInjectionFuture<IDriverServiceClient> driverServiceClient,
            IInjectionFuture<IDriverClientService> driverClientService)
        {
            _driverServiceClient = driverServiceClient;
            _driverClientService = driverClientService;
            _outstandingAlarms = 0;
        }

        public void Dispose()
        {
            _driverServiceClient.Get().OnShutdown();
        }

        public void Dispose(Exception ex)
        {
            _driverServiceClient.Get().OnShutdown(ex);
        }

        public bool IsIdle()
        {
            return _outstandingAlarms == 0 && _alarmDictionary.Count == 0;
        }

        public void ScheduleAlarm(long offset, IObserver<Alarm> handler)
        {
            var alarmId = Guid.NewGuid().ToString();
            _alarmDictionary[alarmId] = handler;
            Log.Log(Level.Info, "Schedule alarm id {0} for {1}", alarmId, offset);
            _driverServiceClient.Get().OnSetAlarm(alarmId, offset);
        }

        public void Run()
        {
            _driverClientService.Get().Start();
            _driverClientService.Get().AwaitTermination();
            Log.Log(Level.Info, "bridge clock terminated");
        }

        public void OnNext(AlarmInfo alarmInfo)
        {
            OnNextAsync(alarmInfo).GetAwaiter().GetResult();
        }

        public async Task OnNextAsync(AlarmInfo alarmInfo)
        {
            Log.Log(Level.Info, "scheduling alarm id {0}", alarmInfo.AlarmId);
            if (_alarmDictionary.TryGetValue(alarmInfo.AlarmId, out IObserver<Alarm> alarmObserver))
            {
                Interlocked.Increment(ref _outstandingAlarms);
                try
                {
                    var alarm = new RuntimeAlarm(alarmInfo.Timestamp, alarmObserver);
                    _alarmDictionary.Remove(alarmInfo.AlarmId);
                    await Task.Run(() => alarm.Handle());
                }
                catch (Exception ex)
                {
                    Log.Log(Level.Error, "Alarm handler raised exception", ex);
                    Dispose(ex);
                }
                finally
                {
                    Interlocked.Decrement(ref _outstandingAlarms);
                }
            }
        }
    }
}