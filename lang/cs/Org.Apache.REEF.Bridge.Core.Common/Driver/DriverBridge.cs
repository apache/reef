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

using Org.Apache.REEF.Bridge.Core.Common.Client.Config;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace Org.Apache.REEF.Bridge.Core.Common.Driver
{
    /// <summary>
    /// DriverBridge is responsible for running application handlers and keeping
    /// track of how many are currently active. It exposes a method <see cref="IsIdle"/>
    /// that indicates if there are any active handlers, which is used to determine
    /// (among other things) whether the driver is currently idle.
    /// </summary>
    internal sealed class DriverBridge
    {
        private static readonly Logger Log = Logger.GetLogger(typeof(DriverBridge));

        // Control event dispatchers

        private readonly DispatchEventHandler<IDriverStarted> _driverStartedDispatcher;

        private readonly DispatchEventHandler<IDriverStopped> _driverStoppedDispatcher;

        // Evaluator event dispatchers

        private readonly DispatchEventHandler<IAllocatedEvaluator> _allocatedEvaluatorDispatcher;

        private readonly DispatchEventHandler<IFailedEvaluator> _failedEvaluatorDispatcher;

        private readonly DispatchEventHandler<ICompletedEvaluator> _completedEvaluatorDispatcher;

        // Context event dispatchers

        private readonly DispatchEventHandler<IActiveContext> _activeContextDispatcher;

        private readonly DispatchEventHandler<IClosedContext> _closedContextDispatcher;

        private readonly DispatchEventHandler<IFailedContext> _failedContextDispatcher;

        private readonly DispatchEventHandler<IContextMessage> _contextMessageDispatcher;

        // Task event dispatchers

        private readonly DispatchEventHandler<ITaskMessage> _taskMessageDispatcher;

        private readonly DispatchEventHandler<IFailedTask> _failedTaskDispatcher;

        private readonly DispatchEventHandler<IRunningTask> _runningTaskDispatcher;

        private readonly DispatchEventHandler<ICompletedTask> _completedTaskDispatcher;

        private readonly DispatchEventHandler<ISuspendedTask> _suspendedTaskDispatcher;

        // Driver restart event dispatchers

        private readonly DispatchEventHandler<IDriverRestarted> _driverRestartedDispatcher;

        private readonly DispatchEventHandler<IActiveContext> _driverRestartActiveContextDispatcher;

        private readonly DispatchEventHandler<IRunningTask> _driverRestartRunningTaskDispatcher;

        private readonly DispatchEventHandler<IDriverRestartCompleted> _driverRestartCompletedDispatcher;

        private readonly DispatchEventHandler<IFailedEvaluator> _driverRestartFailedEvaluatorDispatcher;

        // Client event handlers

        private readonly DispatchEventHandler<byte[]> _clientCloseDispatcher;

        private readonly DispatchEventHandler<byte[]> _clientCloseWithMessageDispatcher;

        private readonly DispatchEventHandler<byte[]> _clientMessageDispatcher;

        private static int s_activeDispatchCounter;

        public static bool IsIdle => s_activeDispatchCounter == 0;

        [Inject]
        private DriverBridge(
            // Runtime events
            [Parameter(Value = typeof(DriverApplicationParameters.DriverStartedHandlers))]
            ISet<IObserver<IDriverStarted>> driverStartHandlers,
            [Parameter(Value = typeof(DriverApplicationParameters.DriverStopHandlers))]
            ISet<IObserver<IDriverStopped>> driverStopHandlers,
            // Evaluator events
            [Parameter(Value = typeof(DriverApplicationParameters.AllocatedEvaluatorHandlers))]
            ISet<IObserver<IAllocatedEvaluator>> allocatedEvaluatorHandlers,
            [Parameter(Value = typeof(DriverApplicationParameters.FailedEvaluatorHandlers))]
            ISet<IObserver<IFailedEvaluator>> failedEvaluatorHandlers,
            [Parameter(Value = typeof(DriverApplicationParameters.CompletedEvaluatorHandlers))]
            ISet<IObserver<ICompletedEvaluator>> completedEvaluatorHandlers,
            // Context events
            [Parameter(Value = typeof(DriverApplicationParameters.ActiveContextHandlers))]
            ISet<IObserver<IActiveContext>> activeContextHandlers,
            [Parameter(Value = typeof(DriverApplicationParameters.ClosedContextHandlers))]
            ISet<IObserver<IClosedContext>> closedContextHandlers,
            [Parameter(Value = typeof(DriverApplicationParameters.FailedContextHandlers))]
            ISet<IObserver<IFailedContext>> failedContextHandlers,
            [Parameter(Value = typeof(DriverApplicationParameters.ContextMessageHandlers))]
            ISet<IObserver<IContextMessage>> contextMessageHandlers,
            // Task events
            [Parameter(Value = typeof(DriverApplicationParameters.TaskMessageHandlers))]
            ISet<IObserver<ITaskMessage>> taskMessageHandlers,
            [Parameter(Value = typeof(DriverApplicationParameters.FailedTaskHandlers))]
            ISet<IObserver<IFailedTask>> failedTaskHandlers,
            [Parameter(Value = typeof(DriverApplicationParameters.RunningTaskHandlers))]
            ISet<IObserver<IRunningTask>> runningTaskHandlers,
            [Parameter(Value = typeof(DriverApplicationParameters.CompletedTaskHandlers))]
            ISet<IObserver<ICompletedTask>> completedTaskHandlers,
            [Parameter(Value = typeof(DriverApplicationParameters.SuspendedTaskHandlers))]
            ISet<IObserver<ISuspendedTask>> suspendedTaskHandlers,
            // Driver restart events
            [Parameter(Value = typeof(DriverApplicationParameters.DriverRestartedHandlers))]
            ISet<IObserver<IDriverRestarted>> driverRestartedHandlers,
            [Parameter(Value = typeof(DriverApplicationParameters.DriverRestartActiveContextHandlers))]
            ISet<IObserver<IActiveContext>> driverRestartActiveContextHandlers,
            [Parameter(Value = typeof(DriverApplicationParameters.DriverRestartRunningTaskHandlers))]
            ISet<IObserver<IRunningTask>> driverRestartRunningTaskHandlers,
            [Parameter(Value = typeof(DriverApplicationParameters.DriverRestartCompletedHandlers))]
            ISet<IObserver<IDriverRestartCompleted>> driverRestartCompletedHandlers,
            [Parameter(Value = typeof(DriverApplicationParameters.DriverRestartFailedEvaluatorHandlers))]
            ISet<IObserver<IFailedEvaluator>> driverRestartFailedEvaluatorHandlers,
            // Client event
            [Parameter(Value = typeof(DriverApplicationParameters.ClientCloseWithMessageHandlers))]
            ISet<IObserver<byte[]>> clientCloseWithMessageHandlers,
            [Parameter(Value = typeof(DriverApplicationParameters.ClientCloseHandlers))]
            ISet<IObserver<byte[]>> clientCloseHandlers,
            [Parameter(Value = typeof(DriverApplicationParameters.ClientMessageHandlers))]
            ISet<IObserver<byte[]>> clientMessageHandlers,
            // Misc.
            [Parameter(Value = typeof(DriverApplicationParameters.TraceListeners))]
            ISet<TraceListener> traceListeners,
            [Parameter(Value = typeof(DriverApplicationParameters.TraceLevel))]
            string traceLevel)
        {
            _driverStartedDispatcher = new DispatchEventHandler<IDriverStarted>(driverStartHandlers);
            _driverStoppedDispatcher = new DispatchEventHandler<IDriverStopped>(driverStopHandlers);
            _allocatedEvaluatorDispatcher = new DispatchEventHandler<IAllocatedEvaluator>(allocatedEvaluatorHandlers);
            _failedEvaluatorDispatcher = new DispatchEventHandler<IFailedEvaluator>(failedEvaluatorHandlers);
            _completedEvaluatorDispatcher = new DispatchEventHandler<ICompletedEvaluator>(completedEvaluatorHandlers);
            _activeContextDispatcher = new DispatchEventHandler<IActiveContext>(activeContextHandlers);
            _closedContextDispatcher = new DispatchEventHandler<IClosedContext>(closedContextHandlers);
            _failedContextDispatcher = new DispatchEventHandler<IFailedContext>(failedContextHandlers);
            _contextMessageDispatcher = new DispatchEventHandler<IContextMessage>(contextMessageHandlers);
            _taskMessageDispatcher = new DispatchEventHandler<ITaskMessage>(taskMessageHandlers);
            _failedTaskDispatcher = new DispatchEventHandler<IFailedTask>(failedTaskHandlers);
            _runningTaskDispatcher = new DispatchEventHandler<IRunningTask>(runningTaskHandlers);
            _completedTaskDispatcher = new DispatchEventHandler<ICompletedTask>(completedTaskHandlers);
            _suspendedTaskDispatcher = new DispatchEventHandler<ISuspendedTask>(suspendedTaskHandlers);
            _driverRestartedDispatcher = new DispatchEventHandler<IDriverRestarted>(driverRestartedHandlers);
            _driverRestartActiveContextDispatcher = new DispatchEventHandler<IActiveContext>(driverRestartActiveContextHandlers);
            _driverRestartRunningTaskDispatcher = new DispatchEventHandler<IRunningTask>(driverRestartRunningTaskHandlers);
            _driverRestartCompletedDispatcher = new DispatchEventHandler<IDriverRestartCompleted>(driverRestartCompletedHandlers);
            _driverRestartFailedEvaluatorDispatcher = new DispatchEventHandler<IFailedEvaluator>(driverRestartFailedEvaluatorHandlers);
            _clientCloseDispatcher = new DispatchEventHandler<byte[]>(clientCloseHandlers);
            _clientCloseWithMessageDispatcher = new DispatchEventHandler<byte[]>(clientCloseWithMessageHandlers);
            _clientMessageDispatcher = new DispatchEventHandler<byte[]>(clientMessageHandlers);

            foreach (var listener in traceListeners)
            {
                Logger.AddTraceListener(listener);
            }
            Log.Log(Level.Info, "Constructing DriverBridge");

            if (Enum.TryParse(traceLevel.ToString(CultureInfo.InvariantCulture), out Level level))
            {
                Logger.SetCustomLevel(level);
            }
            else
            {
                Log.Log(Level.Warning, "Invalid trace level {0} provided, will by default use verbose level", traceLevel);
            }
            s_activeDispatchCounter = 0;
        }

        public async Task DispatchDriverRestartFailedEvaluatorEvent(IFailedEvaluator failedEvaluatorEvent)
        {
            await DispatchAsync(_driverRestartFailedEvaluatorDispatcher, failedEvaluatorEvent);
        }

        public async Task DispatchDriverRestartCompletedEvent(IDriverRestartCompleted driverRestartCompletedEvent)
        {
            await DispatchAsync(_driverRestartCompletedDispatcher, driverRestartCompletedEvent);
        }

        public async Task DispatchDriverRestartRunningTaskEvent(IRunningTask runningTaskEvent)
        {
            await DispatchAsync(_driverRestartRunningTaskDispatcher, runningTaskEvent);
        }

        public async Task DispatchDriverRestartActiveContextEvent(IActiveContext activeContextEvent)
        {
            await DispatchAsync(_driverRestartActiveContextDispatcher, activeContextEvent);
        }

        public async Task DispatchDriverRestartedEvent(IDriverRestarted driverRestartedEvent)
        {
            await DispatchAsync(_driverRestartedDispatcher, driverRestartedEvent);
        }

        public async Task DispatchCompletedTaskEvent(ICompletedTask completedTaskEvent)
        {
            await DispatchAsync(_completedTaskDispatcher, completedTaskEvent);
        }

        public async Task DispatchRunningTaskEvent(IRunningTask runningTaskEvent)
        {
            await DispatchAsync(_runningTaskDispatcher, runningTaskEvent);
        }

        public async Task DispatchFailedTaskEvent(IFailedTask failedTaskEvent)
        {
            await DispatchAsync(_failedTaskDispatcher, failedTaskEvent);
        }

        public async Task DispatchTaskMessageEvent(ITaskMessage taskMessageEvent)
        {
            await DispatchAsync(_taskMessageDispatcher, taskMessageEvent);
        }

        public async Task DispatchSuspendedTaskEvent(ISuspendedTask suspendedTask)
        {
            await DispatchAsync(_suspendedTaskDispatcher, suspendedTask);
        }

        public async Task DispatchContextMessageEvent(IContextMessage contextMessageEvent)
        {
            await DispatchAsync(_contextMessageDispatcher, contextMessageEvent);
        }

        public async Task DispatchFailedContextEvent(IFailedContext failedContextEvent)
        {
            await DispatchAsync(_failedContextDispatcher, failedContextEvent);
        }

        public async Task DispatchClosedContextEvent(IClosedContext closedContextEvent)
        {
            await DispatchAsync(_closedContextDispatcher, closedContextEvent);
        }

        public async Task DispatchActiveContextEvent(IActiveContext activeContextEvent)
        {
            await DispatchAsync(_activeContextDispatcher, activeContextEvent);
        }

        public async Task DispatchCompletedEvaluatorEvent(ICompletedEvaluator completedEvaluatorEvent)
        {
            await DispatchAsync(_completedEvaluatorDispatcher, completedEvaluatorEvent);
        }

        public async Task DispatchFailedEvaluatorEvent(IFailedEvaluator failedEvaluatorEvent)
        {
            await DispatchAsync(_failedEvaluatorDispatcher, failedEvaluatorEvent);
        }

        public async Task DispatchAllocatedEvaluatorEventAsync(IAllocatedEvaluator allocatedEvaluatorEvent)
        {
            await DispatchAsync(_allocatedEvaluatorDispatcher, allocatedEvaluatorEvent);
        }

        public async Task DispatchStartEventAsync(IDriverStarted startEvent)
        {
            await DispatchAsync(_driverStartedDispatcher, startEvent);
        }

        public async Task DispatchStopEvent(IDriverStopped stopEvent)
        {
            await DispatchAsync(_driverStoppedDispatcher, stopEvent);
        }

        public async Task DispatchClientCloseEvent()
        {
            await DispatchAsync(_clientCloseDispatcher, null);
        }

        public async Task DispatchClientCloseWithMessageEvent(byte[] message)
        {
            await DispatchAsync(_clientCloseWithMessageDispatcher, message);
        }

        public async Task DispatchClientMessageEvent(byte[] message)
        {
            await DispatchAsync(_clientMessageDispatcher, message);
        }

        private static async Task DispatchAsync<T>(DispatchEventHandler<T> handler, T message)
        {
            using (var operation = new DisposableOperation(() => handler.OnNext(message)))
            {
                await operation.Run();
            }
        }

        private sealed class DisposableOperation : IDisposable
        {
            private readonly Action _operation;

            public DisposableOperation(Action operation)
            {
                _operation = operation;
            }

            public async Task Run()
            {
                try
                {
                    Interlocked.Increment(ref s_activeDispatchCounter);
                    await Task.Run(_operation);
                }
                catch (Exception ex)
                {
                    Log.Log(Level.Error, "Operation error", ex);
                    throw;
                }
                finally
                {
                    Interlocked.Decrement(ref s_activeDispatchCounter);
                }
            }

            public void Dispose()
            {
            }
        }
    }
}