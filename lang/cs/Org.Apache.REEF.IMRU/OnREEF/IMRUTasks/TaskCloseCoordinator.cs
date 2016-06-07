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
using System.Text;
using System.Threading;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.IMRUTasks
{
    /// <summary>
    /// This class provides a method to handle Task close event. It is called from TaskCloseEventHandler. 
    /// </summary>
    [ThreadSafe]
    internal sealed class TaskCloseCoordinator
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(TaskCloseCoordinator));

        /// <summary>
        /// An event that will wait in close handler to be signaled from Call method.
        /// </summary>
        private readonly ManualResetEventSlim _waitToCloseEvent = new ManualResetEventSlim(false);

        /// <summary>
        /// Handle task close event, wait/signal when closing the task
        /// </summary>
        [Inject]
        private TaskCloseCoordinator()
        {
        }

        /// <summary>
        /// Handle Task close event.
        /// Cancel the CancellationToken for data reading operation, then waiting for the signal from Call method. 
        /// </summary>
        /// <param name="closeEvent"></param>
        /// <param name="cancellationTokenSource"></param>
        internal void HandleEvent(ICloseEvent closeEvent, CancellationTokenSource cancellationTokenSource)
        {
            Logger.Log(Level.Info, "HandleEvent: The task received close event");
            cancellationTokenSource.Cancel();
            _waitToCloseEvent.Wait();

            if (closeEvent.Value.IsPresent())
            {
                Logger.Log(Level.Info, "The task received close event with message: {0}.", Encoding.UTF8.GetString(closeEvent.Value.Value));
            }
            else
            {
                Logger.Log(Level.Info, "The task received close event with no message.");
            }
        }

        /// <summary>
        /// Called from Task right before the task is returned to signals _waitToCloseEvent.
        /// </summary>
        internal void SignalTaskStopped()
        {
            _waitToCloseEvent.Set();
        }
    }
}
