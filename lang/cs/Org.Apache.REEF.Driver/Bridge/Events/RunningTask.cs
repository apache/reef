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
using Org.Apache.REEF.Driver.Bridge.Clr2java;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Bridge.Events
{
    internal sealed class RunningTask : IRunningTask
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(RunningTask));
        private readonly IRunningTaskClr2Java _runningTaskClr2Java;
        private readonly IActiveContextClr2Java _activeContextClr2Java;

        public RunningTask(IRunningTaskClr2Java runningTaskClr2Java)
        {
            using (LOGGER.LogFunction("RunningTask::RunningTask"))
            {
                _runningTaskClr2Java = runningTaskClr2Java;
                _activeContextClr2Java = runningTaskClr2Java.GetActiveContext();
            }
        }

        public Context.IActiveContext ActiveContext
        {
            get
            {
                return new ActiveContext(_activeContextClr2Java);
            }
        }

        public string Id
        {
            get
            {
                return _runningTaskClr2Java.GetId();
            }
        }

        public void Send(byte[] message)
        {
            _runningTaskClr2Java.Send(message);
        }

        public void Suspend(byte[] message)
        {
            _runningTaskClr2Java.Suspend(message);
        }

        public void Suspend()
        {
            Suspend(null);
        }

        public void Dispose(byte[] message)
        {
            _runningTaskClr2Java.Close(message);
        }

        public void Dispose()
        {
            _runningTaskClr2Java.Close(null);
        }
    }
}
