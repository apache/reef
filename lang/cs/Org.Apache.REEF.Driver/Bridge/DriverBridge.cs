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
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Common.Evaluator.DriverConnectionConfigurationProviders;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.Common.Evaluator.Parameters;
using Org.Apache.REEF.Driver.Bridge.Clr2java;
using Org.Apache.REEF.Driver.Bridge.Events;
using Org.Apache.REEF.Driver.Defaults;
using Org.Apache.REEF.Tang.Implementations.InjectionPlan;
using Org.Apache.REEF.Tang.Implementations.Tang;

namespace Org.Apache.REEF.Driver.Bridge
{
    internal sealed class DriverBridge
    {
        private static Logger _logger;
        
        private static ClrSystemHandler<IAllocatedEvaluator> _allocatedEvaluatorSubscriber;

        private static ClrSystemHandler<ITaskMessage> _taskMessageSubscriber;

        private static ClrSystemHandler<IActiveContext> _activeContextSubscriber;

        private static ClrSystemHandler<IActiveContext> _driverRestartActiveContextSubscriber;

        private static ClrSystemHandler<IFailedTask> _failedTaskSubscriber;

        private static ClrSystemHandler<IRunningTask> _runningTaskSubscriber;

        private static ClrSystemHandler<IRunningTask> _driverRestartRunningTaskSubscriber;

        private static ClrSystemHandler<IDriverRestartCompleted> _driverRestartCompletedSubscriber; 

        private static ClrSystemHandler<ISuspendedTask> _suspendedTaskSubscriber;

        private static ClrSystemHandler<IFailedEvaluator> _failedEvaluatorSubscriber;

        private static ClrSystemHandler<IFailedEvaluator> _driverRestartFailedEvaluatorSubscriber;

        private static ClrSystemHandler<ICompletedEvaluator> _completedEvaluatorSubscriber;

        private static ClrSystemHandler<IHttpMessage> _httpServerEventSubscriber;

        private static ClrSystemHandler<ICompletedTask> _completedTaskSubscriber;

        private static ClrSystemHandler<IClosedContext> _closedContextSubscriber;

        private static ClrSystemHandler<IFailedContext> _failedContextSubscriber;

        private static ClrSystemHandler<IContextMessage> _contextMessageSubscriber;

        private static ClrSystemHandler<IDriverRestarted> _driverRestartedSubscriber;

        private readonly ISet<IObserver<IDriverStarted>> _driverStartHandlers;

        private readonly ISet<IObserver<IDriverRestarted>> _driverRestartedHandlers;

        private readonly ISet<IObserver<IEvaluatorRequestor>> _evaluatorRequestHandlers;

        private readonly ISet<IObserver<IAllocatedEvaluator>> _allocatedEvaluatorHandlers;

        private readonly ISet<IObserver<IActiveContext>> _activeContextHandlers;

        private readonly ISet<IObserver<IActiveContext>> _driverRestartActiveContextHandlers;

        private readonly ISet<IObserver<ITaskMessage>> _taskMessageHandlers;

        private readonly ISet<IObserver<IFailedTask>> _failedTaskHandlers;

        private readonly ISet<IObserver<ISuspendedTask>> _suspendedTaskHandlers;

        private readonly ISet<IObserver<IRunningTask>> _runningTaskHandlers;

        private readonly ISet<IObserver<IRunningTask>> _driverRestartRunningTaskHandlers;

        private readonly ISet<IObserver<IDriverRestartCompleted>> _driverRestartCompletedHandlers; 

        private readonly ISet<IObserver<IFailedEvaluator>> _failedEvaluatorHandlers;

        private readonly ISet<IObserver<IFailedEvaluator>> _driverRestartFailedEvaluatorHandlers;

        private readonly ISet<IObserver<ICompletedEvaluator>> _completedEvaluatorHandlers;

        private readonly ISet<IObserver<IClosedContext>> _closedContextHandlers;

        private readonly ISet<IObserver<IFailedContext>> _failedContextHandlers;

        private readonly ISet<IObserver<IContextMessage>> _contextMessageHandlers;

        private readonly ISet<IObserver<ICompletedTask>> _completedTaskHandlers;

        private readonly HttpServerHandler _httpServerHandler;

        private readonly ISet<IConfigurationProvider> _configurationProviders;

        private readonly IProgressProvider _progressProvider;

        [Inject]
        public DriverBridge(
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.DriverStartedHandlers))] ISet<IObserver<IDriverStarted>> driverStartHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.DriverRestartedHandlers))] ISet<IObserver<IDriverRestarted>> driverRestartedHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.EvaluatorRequestHandlers))] ISet<IObserver<IEvaluatorRequestor>> evaluatorRequestHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.AllocatedEvaluatorHandlers))] ISet<IObserver<IAllocatedEvaluator>> allocatedEvaluatorHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.ActiveContextHandlers))] ISet<IObserver<IActiveContext>> activeContextHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.TaskMessageHandlers))] ISet<IObserver<ITaskMessage>> taskMessageHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.FailedTaskHandlers))] ISet<IObserver<IFailedTask>> failedTaskHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.FailedEvaluatorHandlers))] ISet<IObserver<IFailedEvaluator>> failedEvaluatorHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.CompletedEvaluatorHandlers))] ISet<IObserver<ICompletedEvaluator>> completedEvaluatorHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.RunningTaskHandlers))] ISet<IObserver<IRunningTask>> runningTaskHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.CompletedTaskHandlers))] ISet<IObserver<ICompletedTask>> completedTaskHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.SuspendedTaskHandlers))] ISet<IObserver<ISuspendedTask>> suspendedTaskHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.ClosedContextHandlers))] ISet<IObserver<IClosedContext>> closedContextHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.FailedContextHandlers))] ISet<IObserver<IFailedContext>> failedContextHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.ContextMessageHandlers))] ISet<IObserver<IContextMessage>> contextMessageHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.DriverRestartActiveContextHandlers))] ISet<IObserver<IActiveContext>> driverRestartActiveContextHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.DriverRestartRunningTaskHandlers))] ISet<IObserver<IRunningTask>> driverRestartRunningTaskHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.DriverRestartCompletedHandlers))] ISet<IObserver<IDriverRestartCompleted>> driverRestartCompletedHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.DriverRestartFailedEvaluatorHandlers))] ISet<IObserver<IFailedEvaluator>> driverRestartFailedEvaluatorHandlers,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.TraceListenersSet))] ISet<TraceListener> traceListeners,
            [Parameter(Value = typeof(EvaluatorConfigurationProviders))] ISet<IConfigurationProvider> configurationProviders,
            [Parameter(Value = typeof(DriverBridgeConfigurationOptions.TraceLevel))] string traceLevel,
            IDriverReconnConfigProvider driverReconnConfigProvider,
            IDriverConnection driverConnection,
            HttpServerHandler httpServerHandler,
            IProgressProvider progressProvider)
        {
            foreach (TraceListener listener in traceListeners)
            {
                Logger.AddTraceListener(listener);
            }
            _logger = Logger.GetLogger(typeof(DriverBridge));
            _logger.Log(Level.Info, "Constructing DriverBridge");

            Level level;
            if (!Enum.TryParse(traceLevel.ToString(CultureInfo.InvariantCulture), out level))
            {
                _logger.Log(Level.Warning, string.Format(CultureInfo.InvariantCulture, "Invalid trace level {0} provided, will by default use verbose level", traceLevel));
            }
            else
            {
                Logger.SetCustomLevel(level);
            }

            _driverStartHandlers = driverStartHandlers;
            _evaluatorRequestHandlers = evaluatorRequestHandlers;
            _allocatedEvaluatorHandlers = allocatedEvaluatorHandlers;
            _activeContextHandlers = activeContextHandlers;
            _taskMessageHandlers = taskMessageHandlers;
            _failedEvaluatorHandlers = failedEvaluatorHandlers;
            _failedTaskHandlers = failedTaskHandlers;
            _completedTaskHandlers = completedTaskHandlers;
            _runningTaskHandlers = runningTaskHandlers;
            _suspendedTaskHandlers = suspendedTaskHandlers;
            _completedEvaluatorHandlers = completedEvaluatorHandlers;
            _closedContextHandlers = closedContextHandlers;
            _failedContextHandlers = failedContextHandlers;
            _contextMessageHandlers = contextMessageHandlers;
            _driverRestartedHandlers = driverRestartedHandlers;
            _driverRestartActiveContextHandlers = driverRestartActiveContextHandlers;
            _driverRestartRunningTaskHandlers = driverRestartRunningTaskHandlers;
            _driverRestartCompletedHandlers = driverRestartCompletedHandlers;
            _driverRestartFailedEvaluatorHandlers = driverRestartFailedEvaluatorHandlers;
            _httpServerHandler = httpServerHandler;

            // TODO[JIRA REEF-1306]: Remove after it is bound directly into EvaluatorConfigurationProviders.
            _configurationProviders = new HashSet<IConfigurationProvider>(configurationProviders)
            {
                GetDriverReconnectionProvider(driverReconnConfigProvider, driverConnection)
            };

            _progressProvider = progressProvider;
            
            _allocatedEvaluatorSubscriber = new ClrSystemHandler<IAllocatedEvaluator>();
            _completedEvaluatorSubscriber = new ClrSystemHandler<ICompletedEvaluator>();
            _taskMessageSubscriber = new ClrSystemHandler<ITaskMessage>();
            _activeContextSubscriber = new ClrSystemHandler<IActiveContext>();
            _failedTaskSubscriber = new ClrSystemHandler<IFailedTask>();
            _failedEvaluatorSubscriber = new ClrSystemHandler<IFailedEvaluator>();
            _httpServerEventSubscriber = new ClrSystemHandler<IHttpMessage>();
            _completedTaskSubscriber = new ClrSystemHandler<ICompletedTask>();
            _runningTaskSubscriber = new ClrSystemHandler<IRunningTask>();
            _suspendedTaskSubscriber = new ClrSystemHandler<ISuspendedTask>();
            _closedContextSubscriber = new ClrSystemHandler<IClosedContext>();
            _failedContextSubscriber = new ClrSystemHandler<IFailedContext>();
            _contextMessageSubscriber = new ClrSystemHandler<IContextMessage>();
            _driverRestartedSubscriber = new ClrSystemHandler<IDriverRestarted>();
            _driverRestartActiveContextSubscriber = new ClrSystemHandler<IActiveContext>();
            _driverRestartRunningTaskSubscriber = new ClrSystemHandler<IRunningTask>();
            _driverRestartCompletedSubscriber = new ClrSystemHandler<IDriverRestartCompleted>();
            _driverRestartFailedEvaluatorSubscriber = new ClrSystemHandler<IFailedEvaluator>();
        }

        private static IDriverReconnConfigProvider GetDriverReconnectionProvider(
            IDriverReconnConfigProvider driverReconnConfigProvider,
            IDriverConnection driverConnection)
        {
            // If not the default, this means that the user has bound the newer configuration. Return it.
            if (!(driverReconnConfigProvider is DefaultDriverReconnConfigProvider))
            {
                return driverReconnConfigProvider;
            }

            // If not default, this means that the user has bound the old configuration.
            // Use the dynamic configuration provider in that case.
            if (!(driverConnection is MissingDriverConnection))
            {
                return new DynamicDriverReconnConfigProvider(driverConnection.GetType());
            }

            // This is done as a stop gap for deprecation because we cannot bind an implementation 
            // of IDriverConnection to the driver CLRBridgeConfiguration if it is already bound
            // by the user, since the driver configuration and Evaluator configuration will be combined
            // at the Evaluator. We thus need to return the DriverReconnectionConfigurationProvider
            // that does not bind IDriverConnection such that a TANG conflict does not occur.
            return TangFactory.GetTang().NewInjector().GetInstance<DefaultDriverReconnConfigProvider>();
        }

        public BridgeHandlerManager Subscribe()
        {
            var bridgeHandlerManager = new BridgeHandlerManager();

            // subscribe to Allocated Evaluator
            foreach (var handler in _allocatedEvaluatorHandlers)
            {
                _allocatedEvaluatorSubscriber.Subscribe(handler);
                _logger.Log(Level.Verbose, "subscribed to IAllocatedEvaluator handler: " + handler);
            }
            bridgeHandlerManager.AllocatedEvaluatorHandler = ClrHandlerHelper.CreateHandler(_allocatedEvaluatorSubscriber);

            // subscribe to TaskMessage
            foreach (var handler in _taskMessageHandlers)
            {
                _taskMessageSubscriber.Subscribe(handler);
                _logger.Log(Level.Verbose, "subscribed to ITaskMessage handler: " + handler);
            }
            bridgeHandlerManager.TaskMessageHandler = ClrHandlerHelper.CreateHandler(_taskMessageSubscriber);

            // subscribe to Active Context
            foreach (var handler in _activeContextHandlers)
            {
                _activeContextSubscriber.Subscribe(handler);
                _logger.Log(Level.Verbose, "subscribed to IActiveContext handler: " + handler);
            }
            bridgeHandlerManager.ActiveContextHandler = ClrHandlerHelper.CreateHandler(_activeContextSubscriber);

            // subscribe to Failed Task
            foreach (var handler in _failedTaskHandlers)
            {
                _failedTaskSubscriber.Subscribe(handler);
                _logger.Log(Level.Verbose, "subscribed to IFailedTask handler: " + handler);
            }
            bridgeHandlerManager.FailedTaskHandler = ClrHandlerHelper.CreateHandler(_failedTaskSubscriber);

            // subscribe to Running Task
            foreach (var handler in _runningTaskHandlers)
            {
                _runningTaskSubscriber.Subscribe(handler);
                _logger.Log(Level.Verbose, "subscribed to IRunningTask handler: " + handler);
            }
            bridgeHandlerManager.RunningTaskHandler = ClrHandlerHelper.CreateHandler(_runningTaskSubscriber);

            // subscribe to Completed Task
            foreach (var handler in _completedTaskHandlers)
            {
                _completedTaskSubscriber.Subscribe(handler);
                _logger.Log(Level.Verbose, "subscribed to ICompletedTask handler: " + handler);
            }
            bridgeHandlerManager.CompletedTaskHandler = ClrHandlerHelper.CreateHandler(_completedTaskSubscriber);

            // subscribe to Suspended Task
            foreach (var handler in _suspendedTaskHandlers)
            {
                _suspendedTaskSubscriber.Subscribe(handler);
                _logger.Log(Level.Verbose, "subscribed to ISuspendedTask handler: " + handler);
            }
            bridgeHandlerManager.SuspendedTaskHandler = ClrHandlerHelper.CreateHandler(_suspendedTaskSubscriber);

            // subscribe to Failed Evaluator
            foreach (var handler in _failedEvaluatorHandlers)
            {
                _failedEvaluatorSubscriber.Subscribe(handler);
                _logger.Log(Level.Verbose, "subscribed to IFailedEvaluator handler: " + handler);
            }
            bridgeHandlerManager.FailedEvaluatorHandler = ClrHandlerHelper.CreateHandler(_failedEvaluatorSubscriber);

            // subscribe to Completed Evaluator
            foreach (var handler in _completedEvaluatorHandlers)
            {
                _completedEvaluatorSubscriber.Subscribe(handler);
                _logger.Log(Level.Verbose, "subscribed to ICompletedEvaluator handler: " + handler);
            }
            bridgeHandlerManager.CompletedEvaluatorHandler = ClrHandlerHelper.CreateHandler(_completedEvaluatorSubscriber);

            // subscribe to Closed Context
            foreach (var handler in _closedContextHandlers)
            {
                _closedContextSubscriber.Subscribe(handler);
                _logger.Log(Level.Verbose, "subscribed to IClosedContext handler: " + handler);
            }
            bridgeHandlerManager.ClosedContextHandler = ClrHandlerHelper.CreateHandler(_closedContextSubscriber);

            // subscribe to Failed Context
            foreach (var handler in _failedContextHandlers)
            {
                _failedContextSubscriber.Subscribe(handler);
                _logger.Log(Level.Verbose, "subscribed to IFailedContext handler: " + handler);
            }
            bridgeHandlerManager.FailedContextHandler = ClrHandlerHelper.CreateHandler(_failedContextSubscriber);

            // subscribe to Context Message
            foreach (var handler in _contextMessageHandlers)
            {
                _contextMessageSubscriber.Subscribe(handler);
                _logger.Log(Level.Verbose, "subscribed to IContextMesage handler: " + handler);
            }
            bridgeHandlerManager.ContextMessageHandler = ClrHandlerHelper.CreateHandler(_contextMessageSubscriber);
                
            // subscribe to Active Context received during driver restart
            foreach (var handler in _driverRestartActiveContextHandlers)
            {
                _driverRestartActiveContextSubscriber.Subscribe(handler);
                _logger.Log(Level.Verbose, "subscribed to handler for IActiveContext received during driver restart: " + handler);
            }
            bridgeHandlerManager.DriverRestartActiveContextHandler = ClrHandlerHelper.CreateHandler(_driverRestartActiveContextSubscriber);

            // subscribe to Running Task received during driver restart
            foreach (var handler in _driverRestartRunningTaskHandlers)
            {
                _driverRestartRunningTaskSubscriber.Subscribe(handler);
                _logger.Log(Level.Verbose, "subscribed to handler for IRunningTask received during driver restart: " + handler);
            }
            bridgeHandlerManager.DriverRestartRunningTaskHandler = ClrHandlerHelper.CreateHandler(_driverRestartRunningTaskSubscriber);

            // subscribe to Restart Completed received during driver restart
            foreach (var handler in _driverRestartCompletedHandlers)
            {
                _driverRestartCompletedSubscriber.Subscribe(handler);
                _logger.Log(Level.Verbose, "subscribed to handler for IRestartCompleted received during driver restart: " + handler);
            }
            bridgeHandlerManager.DriverRestartCompletedHandler = ClrHandlerHelper.CreateHandler(_driverRestartCompletedSubscriber);

            // subscribe to Failed Evaluator received during driver restart
            foreach (var handler in _driverRestartFailedEvaluatorHandlers)
            {
                _driverRestartFailedEvaluatorSubscriber.Subscribe(handler);
                _logger.Log(Level.Verbose, "subscribed to handler for IFailedEvaluator received during driver restart: " + handler);
            }
            bridgeHandlerManager.DriverRestartFailedEvaluatorHandler = ClrHandlerHelper.CreateHandler(_driverRestartFailedEvaluatorSubscriber);

            // subscribe to Http message
            _httpServerEventSubscriber.Subscribe(_httpServerHandler);
            _logger.Log(Level.Verbose, "subscribed to IHttpMessage handler  :" + _httpServerHandler);
            bridgeHandlerManager.HttpServerHandler = ClrHandlerHelper.CreateHandler(_httpServerEventSubscriber);

            // bind progress provider to provide application progress
            // Bind null handler if user does not specify their own implementation of IProgressProvider. This is
            // used to get around the overhead of Interop calls since the Java side checks for null handler here.
            bridgeHandlerManager.ProgressProvider = _progressProvider is DefaultProgressProvider ? 
                ClrHandlerHelper.CreateNullHandler() : ClrHandlerHelper.CreateHandler(_progressProvider);

            return bridgeHandlerManager;
        }

        /// <summary>
        /// Call start handlers
        /// </summary>
        internal void StartHandlersOnNext(DateTime startTime)
        {
            var driverStarted = new DriverStarted(startTime);
            foreach (var handler in _driverStartHandlers)
            {
                handler.OnNext(driverStarted);
                _logger.Log(Level.Info, "called OnDriverStart handler: " + handler);
            }
        }

        /// <summary>
        /// Call restart handlers
        /// </summary>
        internal void RestartHandlerOnNext(IDriverRestartedClr2Java driverRestartedClr2Java)
        {
            var driverRestarted = new DriverRestarted(driverRestartedClr2Java);
            foreach (var handler in _driverRestartedHandlers)
            {
                handler.OnNext(driverRestarted);
                _logger.Log(Level.Info, "called OnDriverRestarted handler: " + handler);
            }
        }

        internal ISet<IConfigurationProvider> ConfigurationProviders 
        { 
            get { return _configurationProviders; } 
        }
    }
}
