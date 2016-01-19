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
using System.Threading;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Examples.DriverRestart
{
    /// <summary>
    /// A Task that merely prints a greeting and exits.
    /// </summary>
    public sealed class HelloRestartTask : ITask, IDriverMessageHandler, IDriverConnectionMessageHandler
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(HelloRestartTask));
        private bool _exit;

        [Inject]
        private HelloRestartTask()
        {
            _exit = false;
        }

        public void Dispose()
        {
        }

        public byte[] Call(byte[] memento)
        {
            while (!_exit)
            {
                Thread.Sleep(5000);
            }

            return null;
        }

        public void Handle(IDriverMessage message)
        {
            Logger.Log(Level.Verbose, "Received a message from driver. We should exit now...");
            _exit = true;
        }

        public void OnNext(IDriverConnectionMessage value)
        {
            switch (value.State)
            {
                case DriverConnectionState.Disconnected:
                    Logger.Log(Level.Warning, "Task lost connection with Driver!");
                    break;
                case DriverConnectionState.Reconnected:
                    Logger.Log(Level.Info, "Task reconnected with new Driver!");
                    break;
                default:
                    Logger.Log(Level.Warning, "Task driver connection status: " + value.State);
                    break;
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
}