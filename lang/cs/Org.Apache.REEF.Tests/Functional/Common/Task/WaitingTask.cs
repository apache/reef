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

using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Tests.Functional.Common.Task
{
    /// <summary>
    /// A helper test class that implements <see cref="ITask"/>, which logs
    /// messages provided by the caller of the constructor and waits for an 
    /// <see cref="EventMonitor"/> to be signaled.
    /// </summary>
    public abstract class WaitingTask : ITask
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(WaitingTask));

        private readonly string _messageToLogPreWait;
        private readonly string _messageToLogPostWait;
        private readonly EventMonitor _eventMonitor;

        protected WaitingTask(
            EventMonitor eventMonitor,
            string messageToLogPreWait = null,
            string messageToLogPostWait = null)
        {
            _eventMonitor = eventMonitor;
            _messageToLogPreWait = messageToLogPreWait;
            _messageToLogPostWait = messageToLogPostWait;
        }

        public byte[] Call(byte[] memento)
        {
            if (!string.IsNullOrWhiteSpace(_messageToLogPreWait))
            {
                Logger.Log(Level.Info, _messageToLogPreWait);
            }

            _eventMonitor.Wait();

            if (!string.IsNullOrWhiteSpace(_messageToLogPostWait))
            {
                Logger.Log(Level.Info, _messageToLogPostWait);
            }

            return null;
        }

        public void Dispose()
        {
        }
    }
}