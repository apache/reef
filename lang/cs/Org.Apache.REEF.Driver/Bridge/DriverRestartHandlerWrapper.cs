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
using Org.Apache.REEF.Driver.Bridge.Events;
using Org.Apache.REEF.Wake.Time.Event;

namespace Org.Apache.REEF.Driver.Bridge
{
    /// <summary>
    /// Wrapper of the new Observers of DriverRestarted with and into an Observer of StartTime.
    /// </summary>
    /// <remarks>
    /// Rationale: This way, we don't have to change the C++ code in the same change as the API.
    /// </remarks>
    internal sealed class DriverRestartHandlerWrapper : IObserver<StartTime>
    {
        private readonly IObserver<IDriverRestarted> _driverRestartedObserver;
        private readonly IObserver<StartTime> _startTimeObserver;

        internal DriverRestartHandlerWrapper(IObserver<StartTime> startTimeObserver,
            IObserver<IDriverRestarted> driverRestartedObserver)
        {
            _startTimeObserver = startTimeObserver;
            _driverRestartedObserver = driverRestartedObserver;
        }

        public void OnNext(StartTime startTime)
        {
            _driverRestartedObserver.OnNext(new DriverRestarted(new DateTime(startTime.TimeStamp)));
            _startTimeObserver.OnNext(startTime);
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