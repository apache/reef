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

using System.Threading;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Tests.Functional.Common
{
    /// <summary>
    /// An test EventHandle that simply wraps around a <see cref="ManualResetEventSlim"/>.
    /// </summary>
    public sealed class EventHandle
    {
        private readonly ManualResetEventSlim _eventHandle = new ManualResetEventSlim();

        [Inject]
        private EventHandle()
        {
        }

        /// <summary>
        /// Sets the Event.
        /// </summary>
        public void Signal()
        {
            _eventHandle.Set();
        }

        /// <summary>
        /// Waits for the event signal.
        /// </summary>
        public void Wait()
        {
            _eventHandle.Wait();
        }

        /// <summary>
        /// Resets the event signal.
        /// </summary>
        public void Reset()
        {
            _eventHandle.Reset();
        }
    }
}