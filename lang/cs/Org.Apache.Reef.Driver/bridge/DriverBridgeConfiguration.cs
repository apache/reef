/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using Org.Apache.Reef.Common.Context;
using Org.Apache.Reef.Common.Evaluator;
using Org.Apache.Reef.Driver.Context;
using Org.Apache.Reef.Driver.Evaluator;
using Org.Apache.Reef.Driver.Task;
using Org.Apache.Reef.Utilities.Logging;
using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Tang.Formats;
using Org.Apache.Reef.Tang.Util;
using Org.Apache.Reef.Wake.Time;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;

[module: SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1401:FieldsMustBePrivate", Justification = "static field, typical usage in configurations")]

namespace Org.Apache.Reef.Driver.Bridge
{
    public class DriverBridgeConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        ///  The event handler invoked right after the driver boots up. 
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly RequiredImpl<IStartHandler> OnDriverStarted = new RequiredImpl<IStartHandler>();

        /// <summary>
        ///  The event handler invoked when driver restarts
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<StartTime>> OnDriverRestarted = new OptionalImpl<IObserver<StartTime>>();

        /// <summary>
        /// The event handler for requesting evaluator
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<IEvaluatorRequestor>> OnEvaluatorRequested = new OptionalImpl<IObserver<IEvaluatorRequestor>>();

        /// <summary>
        /// Event handler for allocated evaluators. Defaults to returning the evaluator if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<IAllocatedEvaluator>> OnEvaluatorAllocated = new OptionalImpl<IObserver<IAllocatedEvaluator>>();

        /// <summary>
        /// Event handler for completed evaluators. Defaults to logging if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<ICompletedEvaluator>> OnEvaluatorCompleted = new OptionalImpl<IObserver<ICompletedEvaluator>>();

        /// <summary>
        /// Event handler for failed evaluators. Defaults to job failure if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<IFailedEvaluator>> OnEvaluatorFailed = new OptionalImpl<IObserver<IFailedEvaluator>>();

        /// <summary>
        /// Event handler for failed evaluators. Defaults to job failure if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IHttpHandler> OnHttpEvent = new OptionalImpl<IHttpHandler>();

        /// <summary>
        /// Event handler for task messages. Defaults to logging if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<ITaskMessage>> OnTaskMessage = new OptionalImpl<IObserver<ITaskMessage>>();

        /// <summary>
        /// Event handler for completed tasks. Defaults to closing the context the task ran on if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]        
        public static readonly OptionalImpl<IObserver<ICompletedTask>> OnTaskCompleted = new OptionalImpl<IObserver<ICompletedTask>>();

        /// <summary>
        /// Event handler for failed tasks. Defaults to job failure if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<IFailedTask>> OnTaskFailed = new OptionalImpl<IObserver<IFailedTask>>();

        ///// <summary>
        ///// Event handler for running tasks. Defaults to logging if not bound.
        ///// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<IRunningTask>> OnTaskRunning = new OptionalImpl<IObserver<IRunningTask>>();

        ///// <summary>
        ///// Event handler for running task received during driver restart. Defaults to logging if not bound.
        ///// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<IRunningTask>> OnDriverRestartTaskRunning = new OptionalImpl<IObserver<IRunningTask>>();

        /// <summary>
        /// Event handler for suspended tasks. Defaults to job failure if not bound. Rationale: many jobs don't support
        /// task suspension. Hence, this parameter should be optional. The only sane default is to crash the job, then.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<ISuspendedTask>> OnTaskSuspended = new OptionalImpl<IObserver<ISuspendedTask>>();

        /// <summary>
        /// Event handler for active context. Defaults to closing the context if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<IActiveContext>> OnContextActive = new OptionalImpl<IObserver<IActiveContext>>();

        /// <summary>
        /// Event handler for active context received during driver restart. Defaults to closing the context if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<IActiveContext>> OnDirverRestartContextActive = new OptionalImpl<IObserver<IActiveContext>>();

        /// <summary>
        /// Event handler for closed context. Defaults to logging if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<IClosedContext>> OnContextClosed = new OptionalImpl<IObserver<IClosedContext>>();

        /// <summary>
        ///  Event handler for closed context. Defaults to job failure if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<IFailedContext>> OnContextFailed = new OptionalImpl<IObserver<IFailedContext>>();

        /// <summary>
        ///  Event handler for context messages. Defaults to logging if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<IContextMessage>> OnContextMessage = new OptionalImpl<IObserver<IContextMessage>>();

        /// <summary>
        /// Additional set of string arguments that can be pssed to handlers through client
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalParameter<string> CommandLineArguments = new OptionalParameter<string>();

        /// <summary>
        /// The trace level of the TraceListner
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalParameter<string> CustomTraceLevel = new OptionalParameter<string>();

        /// <summary>
        /// Additional set of trace listners provided by client
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalParameter<TraceListener> CustomTraceListeners = new OptionalParameter<TraceListener>();

        /// <summary>
        ///  The implemenation for (attempting to) re-establish connection to driver
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IDriverConnection> OnDriverReconnect = new OptionalImpl<IDriverConnection>();

        // This is currently not needed in Bridge/Driver model
        ///// <summary>
        ///// The event handler invoked right before the driver shuts down. Defaults to ignore.
        ///// </summary>
        //public static readonly OptionalImpl<IObserver<StopTime>> OnDriverStop = new OptionalImpl<IObserver<StopTime>>();

        // Client handlers only needed when client interactions are expeceted. Not enabled for now.
        ///// <summary>
        ///// Event handler for client messages. Defaults to logging if not bound.
        ///// </summary>
        //public static readonly OptionalImpl<IObserver<byte[]>> OnClientMessage = new OptionalImpl<IObserver<byte[]>>();

        // Client handlers only needed when client interactions are expeceted. Not enabled for now.
        ///// <summary>
        ///// Event handler for close messages sent by the client. Defaults to job failure if not bound.
        ///// Note: in java the type is void, but IObserver does not take void as a type
        ///// </summary>
        //public static readonly OptionalImpl<IObserver<byte[]>> OnClientClosed = new OptionalImpl<IObserver<byte[]>>();

        // Client handlers only needed when client interactions are expeceted. Not enabled for now.
        ///// <summary>
        ///// Event handler for close messages sent by the client. Defaults to job failure if not bound.
        ///// </summary>
        //public static readonly OptionalImpl<IObserver<byte[]>> OnClientClosedMessage = new OptionalImpl<IObserver<byte[]>>();

        public static ConfigurationModule ConfigurationModule
        {
            get
            {
                return new DriverBridgeConfiguration()
                .BindImplementation(GenericType<IStartHandler>.Class, OnDriverStarted)
                .BindNamedParameter(GenericType<DriverBridgeConfigurationOptions.DriverRestartHandler>.Class, OnDriverRestarted)
                .BindImplementation(GenericType<IDriverConnection>.Class, OnDriverReconnect)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.EvaluatorRequestHandlers>.Class, OnEvaluatorRequested)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.AllocatedEvaluatorHandlers>.Class, OnEvaluatorAllocated)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.ActiveContextHandlers>.Class, OnContextActive)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.TaskMessageHandlers>.Class, OnTaskMessage)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.FailedTaskHandlers>.Class, OnTaskFailed)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.RunningTaskHandlers>.Class, OnTaskRunning)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.SuspendedTaskHandlers>.Class, OnTaskSuspended)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.FailedEvaluatorHandlers>.Class, OnEvaluatorFailed)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.CompletedEvaluatorHandlers>.Class, OnEvaluatorCompleted)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.CompletedTaskHandlers>.Class, OnTaskCompleted)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.ClosedContextHandlers>.Class, OnContextClosed)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.FailedContextHandlers>.Class, OnContextFailed)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.ContextMessageHandlers>.Class, OnContextMessage)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.ArgumentSets>.Class, CommandLineArguments)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.HttpEventHandlers>.Class, OnHttpEvent)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.TraceListenersSet>.Class, CustomTraceListeners)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.DriverRestartActiveContextHandlers>.Class, OnDirverRestartContextActive)
                .BindSetEntry(GenericType<DriverBridgeConfigurationOptions.DriverRestartRunningTaskHandlers>.Class, OnDriverRestartTaskRunning)
                .BindNamedParameter(GenericType<DriverBridgeConfigurationOptions.TraceLevel>.Class, CustomTraceLevel)
                .Build();
            }
        }
    }

    public class CommandLineArguments
    {
        [Inject]
        public CommandLineArguments([Parameter(typeof(DriverBridgeConfigurationOptions.ArgumentSets))] ISet<string> arguments)
        {
            Arguments = arguments;
        }

        public ISet<string> Arguments { get; set; }
    }

    public class CustomTraceListeners
    {
        [Inject]
        public CustomTraceListeners([Parameter(typeof(DriverBridgeConfigurationOptions.TraceListenersSet))] ISet<TraceListener> listeners)
        {
            Listeners = listeners;
        }

        public ISet<TraceListener> Listeners { get; set; }
    }

    public class CustomTraceLevel
    {
        [Inject]
        public CustomTraceLevel([Parameter(typeof(DriverBridgeConfigurationOptions.TraceLevel))] string traceLevel)
        {
            Level level = Level.Verbose;
            if (Enum.TryParse(traceLevel.ToString(CultureInfo.InvariantCulture), out level))
            {
                Logger.SetCustomLevel(level);
            }
            else
            {
                Console.WriteLine("Cannot parse trace level" + traceLevel);
            }
            TraceLevel = level;
        }

        public Level TraceLevel { get; set; }
    }
}
