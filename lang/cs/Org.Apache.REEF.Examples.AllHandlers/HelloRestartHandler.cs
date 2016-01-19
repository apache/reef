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
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Wake.Time.Event;

namespace Org.Apache.REEF.Examples.AllHandlers
{
    /// <summary>
    /// A sample implementation of driver restart handler
    /// </summary>
    public class HelloRestartHandler : IObserver<IDriverRestarted>
    {
        [Inject]
        private HelloRestartHandler()
        {
        }

        /// <summary>
        /// It is called when the driver is restarted
        /// </summary>
        /// <param name="value"></param>
        public void OnNext(IDriverRestarted value)
        {
            Console.WriteLine("Hello from CLR: we are informed that Driver has restarted at " + new DateTime(value.StartTime.Ticks));
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
