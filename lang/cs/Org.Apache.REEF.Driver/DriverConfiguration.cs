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
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Driver.Bridge;
using Org.Apache.REEF.Driver.Context;
using Org.Apache.REEF.Driver.Evaluator;
using Org.Apache.REEF.Driver.Task;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Driver
{
    /// <summary>
    /// Fill this out to configure a Driver.
    /// </summary>
    [ClientSide]
    public sealed class DriverConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        /// The event handler called after the Driver started.
        /// </summary>
        public static readonly RequiredImpl<IObserver<IDriverStarted>> OnDriverStarted =
            new RequiredImpl<IObserver<IDriverStarted>>();

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
        /// Event handler for for HTTP calls to the Driver's HTTP server.
        /// </summary>
        public static readonly OptionalImpl<IHttpHandler> OnHttpEvent = new OptionalImpl<IHttpHandler>();

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
        /// Additional set of string arguments that can be passed to handlers through client
        /// </summary>
        public static readonly OptionalParameter<string> CommandLineArguments = new OptionalParameter<string>();

        /// <summary>
        /// The trace level of the TraceListener
        /// </summary>
        public static readonly OptionalParameter<string> CustomTraceLevel = new OptionalParameter<string>();

        /// <summary>
        /// Additional set of trace listeners provided by client
        /// </summary>
        public static readonly OptionalParameter<TraceListener> CustomTraceListeners =
            new OptionalParameter<TraceListener>();

        /// <summary>
        /// The number of times the application should be submitted in case of failures
        /// </summary>
        [Obsolete("Deprecated in 0.14, will be removed.")]
        public static readonly OptionalParameter<int> MaxApplicationSubmissions =
            new OptionalParameter<int>();

        /// <summary>
        /// The implemenation for (attempting to) re-establish connection to driver
        /// </summary>
        public static readonly OptionalImpl<IDriverConnection> OnDriverReconnect = new OptionalImpl<IDriverConnection>();

        /// <summary>
        /// Evaluator recovery timeout for driver restart in seconds. If value is greater than 0, restart is enabled. The default value is -1.
        /// </summary>
        public static readonly OptionalParameter<int> DriverRestartEvaluatorRecoverySeconds = new OptionalParameter<int>();

        /// <summary>
        /// The progress provider that will be injected at the Driver.
        /// </summary>
        public static readonly OptionalImpl<IProgressProvider> ProgressProvider = new OptionalImpl<IProgressProvider>();

        public static ConfigurationModule ConfigurationModule
        {
            get
            {
                return new DriverConfiguration()
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.DriverStartedHandlers>.Class,
                        OnDriverStarted)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.DriverRestartedHandlers>.Class,
                        OnDriverRestarted)
                    .BindImplementation(GenericType<IDriverConnection>.Class, OnDriverReconnect)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.AllocatedEvaluatorHandlers>.Class,
                        OnEvaluatorAllocated)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.ActiveContextHandlers>.Class,
                        OnContextActive)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.TaskMessageHandlers>.Class, OnTaskMessage)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.FailedTaskHandlers>.Class, OnTaskFailed)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.RunningTaskHandlers>.Class, OnTaskRunning)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.SuspendedTaskHandlers>.Class,
                        OnTaskSuspended)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.FailedEvaluatorHandlers>.Class,
                        OnEvaluatorFailed)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.CompletedEvaluatorHandlers>.Class,
                        OnEvaluatorCompleted)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.CompletedTaskHandlers>.Class,
                        OnTaskCompleted)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.ClosedContextHandlers>.Class,
                        OnContextClosed)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.FailedContextHandlers>.Class,
                        OnContextFailed)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.ContextMessageHandlers>.Class,
                        OnContextMessage)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.DriverRestartCompletedHandlers>.Class,
                        OnDriverRestartCompleted)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.ArgumentSets>.Class, CommandLineArguments)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.HttpEventHandlers>.Class, OnHttpEvent)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.TraceListenersSet>.Class,
                        CustomTraceListeners)
                    .BindSetEntry(
                        GenericType<DriverBridgeConfigurationOptions.DriverRestartActiveContextHandlers>.Class,
                        OnDriverRestartContextActive)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.DriverRestartRunningTaskHandlers>.Class,
                        OnDriverRestartTaskRunning)
                    .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.DriverRestartFailedEvaluatorHandlers>.Class,
                        OnDriverRestartEvaluatorFailed)
                    .BindNamedParameter(GenericType<DriverBridgeConfigurationOptions.TraceLevel>.Class, CustomTraceLevel)
                    .BindNamedParameter(GenericType<DriverBridgeConfigurationOptions.MaxApplicationSubmissions>.Class,
                        MaxApplicationSubmissions)
                    .BindNamedParameter(GenericType<DriverBridgeConfigurationOptions.DriverRestartEvaluatorRecoverySeconds>.Class,
                        DriverRestartEvaluatorRecoverySeconds)
                    .BindImplementation(GenericType<IProgressProvider>.Class, ProgressProvider)
                    .Build();
            }
        }
    }
}