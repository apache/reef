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
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Driver.Defaults
{
    /// <summary>
    ///  Default event handler for TaskRuntime received during driver restart: Logging it.
    /// </summary>
    public class DefaultDriverRestartTaskRunningHandler : IObserver<IRunningTask>
    {
        private static readonly Logger LOGGER = Logger.GetLogger(typeof(DefaultDriverRestartTaskRunningHandler));
        
        [Inject]
        public DefaultDriverRestartTaskRunningHandler()
        {
        }

        public void OnNext(IRunningTask runningTask)
        {
            LOGGER.Log(Level.Info, "Received TaskRuntime during driver restart: " + runningTask.Id);
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
