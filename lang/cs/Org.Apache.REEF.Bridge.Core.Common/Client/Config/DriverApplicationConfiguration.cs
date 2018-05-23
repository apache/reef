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
using System.Diagnostics;
using Org.Apache.REEF.Bridge.Core.Common.Driver;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Wake.Time;

namespace Org.Apache.REEF.Bridge.Core.Common.Client.Config
{
    /// <summary>
    /// Fill this out to configure a Driver.
    /// </summary>
    [ClientSide]
    public sealed class DriverApplicationConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        /// The event handler called after the Driver started.
        /// </summary>
        public static readonly RequiredImpl<IObserver<IDriverStarted>> OnDriverStarted =
            new RequiredImpl<IObserver<IDriverStarted>>();

        /// <summary>
        /// The event handler called when the Driver has been stopped.
        /// </summary>
        public static readonly OptionalImpl<IObserver<IDriverStopped>> OnDriverStopped =
            new OptionalImpl<IObserver<IDriverStopped>>();

        /// <summary>
        /// The event handler invoked when driver restarts
        /// </summary>
        public static readonly OptionalImpl<IObserver<IDriverRestarted>> OnDriverRestarted =
            new OptionalImpl<IObserver<IDriverRestarted>>();

        /// <summary>
        /// Event handler for allocated evaluators. Defaults to returning the evaluator if not bound.
        /// </summary>
        public static readonly OptionalImpl<IObserver<IAllocatedEvaluator>> OnEvaluatorAllocated =
            new OptionalImpl<IObserver<IAllocatedEvaluator>>();

        /// <summary>
        /// Event handler for completed evaluators. Defaults to logging if not bound.
        /// </summary>
        public static readonly OptionalImpl<IObserver<ICompletedEvaluator>> OnEvaluatorCompleted =
            new OptionalImpl<IObserver<ICompletedEvaluator>>();

        /// <summary>
        /// Event handler for failed evaluators. Defaults to job failure if not bound.
        /// </summary>
        public static readonly OptionalImpl<IObserver<IFailedEvaluator>> OnEvaluatorFailed =
            new OptionalImpl<IObserver<IFailedEvaluator>>();

        /// <summary>
        /// Event handler for task messages. Defaults to logging if not bound.
        /// </summary>
        public static readonly OptionalImpl<IObserver<ITaskMessage>> OnTaskMessage =
            new OptionalImpl<IObserver<ITaskMessage>>();

        /// <summary>
        /// Event handler for completed tasks. Defaults to closing the context the task ran on if not bound.
        /// </summary>
        public static readonly OptionalImpl<IObserver<ICompletedTask>> OnTaskCompleted =
            new OptionalImpl<IObserver<ICompletedTask>>();

        /// <summary>
        /// Event handler for failed tasks. Defaults to job failure if not bound.
        /// </summary>
        public static readonly OptionalImpl<IObserver<IFailedTask>> OnTaskFailed =
            new OptionalImpl<IObserver<IFailedTask>>();

        ///// <summary>
        ///// Event handler for running tasks. Defaults to logging if not bound.
        ///// </summary>
        public static readonly OptionalImpl<IObserver<IRunningTask>> OnTaskRunning =
            new OptionalImpl<IObserver<IRunningTask>>();

        ///// <summary>
        ///// Event handler for running task received during driver restart. Defaults to logging if not bound.
        ///// </summary>
        public static readonly OptionalImpl<IObserver<IRunningTask>> OnDriverRestartTaskRunning =
            new OptionalImpl<IObserver<IRunningTask>>();

        /// <summary>
        /// Event handler for suspended tasks. Defaults to job failure if not bound.
        /// </summary>
        /// <remarks>
        /// Rationale: many jobs don't support task suspension. Hence, this parameter should be optional. The only sane default is
        /// to crash the job, then.
        /// </remarks>
        public static readonly OptionalImpl<IObserver<ISuspendedTask>> OnTaskSuspended =
            new OptionalImpl<IObserver<ISuspendedTask>>();

        /// <summary>
        /// Event handler for active context. Defaults to closing the context if not bound.
        /// </summary>
        public static readonly OptionalImpl<IObserver<IActiveContext>> OnContextActive =
            new OptionalImpl<IObserver<IActiveContext>>();

        /// <summary>
        /// Event handler for active context received during driver restart. Defaults to closing the context if not bound.
        /// </summary>
        public static readonly OptionalImpl<IObserver<IActiveContext>> OnDriverRestartContextActive =
            new OptionalImpl<IObserver<IActiveContext>>();

        /// <summary>
        /// Event handler for closed context. Defaults to logging if not bound.
        /// </summary>
        public static readonly OptionalImpl<IObserver<IClosedContext>> OnContextClosed =
            new OptionalImpl<IObserver<IClosedContext>>();

        /// <summary>
        /// Event handler for closed context. Defaults to job failure if not bound.
        /// </summary>
        public static readonly OptionalImpl<IObserver<IFailedContext>> OnContextFailed =
            new OptionalImpl<IObserver<IFailedContext>>();

        /// <summary>
        /// Event handler for context messages. Defaults to logging if not bound.
        /// </summary>
        public static readonly OptionalImpl<IObserver<IContextMessage>> OnContextMessage =
            new OptionalImpl<IObserver<IContextMessage>>();

        /// <summary>
        /// Event handler for driver restart completed. Defaults to logging if not bound.
        /// </summary>
        public static readonly OptionalImpl<IObserver<IDriverRestartCompleted>> OnDriverRestartCompleted =
            new OptionalImpl<IObserver<IDriverRestartCompleted>>();

        ///// <summary>
        ///// Event handler for driver restart failed evaluator event received during driver restart. Defaults to job failure if not bound.
        ///// </summary>
        public static readonly OptionalImpl<IObserver<IFailedEvaluator>> OnDriverRestartEvaluatorFailed =
            new OptionalImpl<IObserver<IFailedEvaluator>>();

        /// <summary>
        /// Event handler for client close.
        /// </summary>
        public static readonly OptionalImpl<IObserver<byte[]>> OnClientClose = new OptionalImpl<IObserver<byte[]>>();

        /// <summary>
        /// Event handler for client close with message.
        /// </summary>
        public static readonly OptionalImpl<IObserver<byte[]>> OnClientCloseWithMessage = new OptionalImpl<IObserver<byte[]>>();

        /// <summary>
        /// Event handler for client message.
        /// </summary>
        public static readonly OptionalImpl<IObserver<byte[]>> OnClientMessage = new OptionalImpl<IObserver<byte[]>>();

        /// <summary>
        /// The trace level of the TraceListener
        /// </summary>
        public static readonly OptionalParameter<string> CustomTraceLevel = new OptionalParameter<string>();

        /// <summary>
        /// Additional set of trace listeners provided by client
        /// </summary>
        public static readonly OptionalParameter<TraceListener> CustomTraceListeners =
            new OptionalParameter<TraceListener>();

        public static ConfigurationModule ConfigurationModule => new DriverApplicationConfiguration()
            .BindImplementation(GenericType<IClock>.Class, GenericType<BridgeClock>.Class)
            .BindImplementation(GenericType<IEvaluatorRequestor>.Class,
                GenericType<DriverBridgeEvaluatorRequestor>.Class)

            // Event handlers
            .BindSetEntry(GenericType<DriverApplicationParameters.DriverStartedHandlers>.Class, OnDriverStarted)
            .BindSetEntry(GenericType<DriverApplicationParameters.DriverStopHandlers>.Class, OnDriverStopped)
            .BindSetEntry(GenericType<DriverApplicationParameters.AllocatedEvaluatorHandlers>.Class, OnEvaluatorAllocated)
            .BindSetEntry(GenericType<DriverApplicationParameters.ActiveContextHandlers>.Class, OnContextActive)
            .BindSetEntry(GenericType<DriverApplicationParameters.TaskMessageHandlers>.Class, OnTaskMessage)
            .BindSetEntry(GenericType<DriverApplicationParameters.FailedTaskHandlers>.Class, OnTaskFailed)
            .BindSetEntry(GenericType<DriverApplicationParameters.RunningTaskHandlers>.Class, OnTaskRunning)
            .BindSetEntry(GenericType<DriverApplicationParameters.SuspendedTaskHandlers>.Class, OnTaskSuspended)
            .BindSetEntry(GenericType<DriverApplicationParameters.FailedEvaluatorHandlers>.Class, OnEvaluatorFailed)
            .BindSetEntry(GenericType<DriverApplicationParameters.CompletedEvaluatorHandlers>.Class, OnEvaluatorCompleted)
            .BindSetEntry(GenericType<DriverApplicationParameters.CompletedTaskHandlers>.Class, OnTaskCompleted)
            .BindSetEntry(GenericType<DriverApplicationParameters.ClosedContextHandlers>.Class, OnContextClosed)
            .BindSetEntry(GenericType<DriverApplicationParameters.FailedContextHandlers>.Class, OnContextFailed)
            .BindSetEntry(GenericType<DriverApplicationParameters.ContextMessageHandlers>.Class, OnContextMessage)
            .BindSetEntry(GenericType<DriverApplicationParameters.DriverRestartCompletedHandlers>.Class,
                OnDriverRestartCompleted)
            .BindSetEntry(GenericType<DriverApplicationParameters.DriverRestartedHandlers>.Class, OnDriverRestarted)
            .BindSetEntry(GenericType<DriverApplicationParameters.DriverRestartActiveContextHandlers>.Class,
                OnDriverRestartContextActive)
            .BindSetEntry(GenericType<DriverApplicationParameters.DriverRestartRunningTaskHandlers>.Class,
                OnDriverRestartTaskRunning)
            .BindSetEntry(GenericType<DriverApplicationParameters.DriverRestartFailedEvaluatorHandlers>.Class,
                OnDriverRestartEvaluatorFailed)
            .BindSetEntry(GenericType<DriverApplicationParameters.ClientCloseHandlers>.Class, OnClientClose)
            .BindSetEntry(GenericType<DriverApplicationParameters.ClientCloseWithMessageHandlers>.Class, OnClientCloseWithMessage)
            .BindSetEntry(GenericType<DriverApplicationParameters.ClientMessageHandlers>.Class, OnClientMessage)
            .BindSetEntry(GenericType<DriverApplicationParameters.TraceListeners>.Class, CustomTraceListeners)
            .BindNamedParameter(GenericType<DriverApplicationParameters.TraceLevel>.Class, CustomTraceLevel)
            .Build();
    }
}
