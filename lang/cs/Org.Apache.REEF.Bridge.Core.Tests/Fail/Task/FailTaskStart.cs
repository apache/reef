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
using Org.Apache.REEF.Bridge.Core.Tests.Fail.ThreadInterruptedException;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Bridge.Core.Tests.Fail.Task
{
    internal sealed class FailTaskStart : BlockingCallTaskBase, IObserver<ITaskStart>
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(FailTaskStart));

        [Inject]
        private FailTaskStart()
        {
            Log.Log(Level.Info, "task created");
        }

        public void OnNext(ITaskStart value)
        {
            SimulatedTaskFailure ex = new SimulatedTaskFailure("FailTaskMsg.send() invoked.");
            Log.Log(Level.Info, "FailTaskMsg.send() invoked: {0}", ex);
            throw ex;
        }
    }
}
