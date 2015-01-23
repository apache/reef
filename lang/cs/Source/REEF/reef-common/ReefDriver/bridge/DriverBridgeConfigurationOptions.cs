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
using Org.Apache.Reef.Driver.Context;
using Org.Apache.Reef.Driver.Defaults;
using Org.Apache.Reef.Driver.Evaluator;
using Org.Apache.Reef.Driver.Task;
using Org.Apache.Reef.Tang.Annotations;
using Org.Apache.Reef.Wake.Time;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

[module: SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1402:FileMayOnlyContainASingleClass", Justification = "allow name parameter class to be embedded")]

namespace Org.Apache.Reef.Driver.Bridge
{
    /// <summary>
    /// Hosts all named parameters for Drivers, including bridge handlers.
    /// </summary>
    public class DriverBridgeConfigurationOptions
    {
        // Level.Verbose (since enum is not suppoted for TANG, we use a string here)
        private const string _verboseLevel = "Verbose";

        [NamedParameter(documentation: "Called when driver is restarted, after CLR bridge is set up.", defaultClasses: new[] { typeof(DefaultDriverRestartHandler) })]
        public class DriverRestartHandler : Name<IObserver<StartTime>>
        {
        }

        [NamedParameter(documentation: "Called when evaluator is requested.", defaultClasses: new[] { typeof(DefaultEvaluatorRequestorHandler) })] 
        public class EvaluatorRequestHandlers : Name<ISet<IObserver<IEvaluatorRequestor>>>
        {
        }

        [NamedParameter(documentation: "Called when an exception occurs on a running evaluator.", defaultClasses: new[] { typeof(DefaultEvaluatorFailureHandler) })]
        public class FailedEvaluatorHandlers : Name<ISet<IObserver<IFailedEvaluator>>>
        {
        }

        [NamedParameter(documentation: "Called when an evaluator completes.", defaultClasses: new[] { typeof(DefaultEvaluatorCompletionHandler) })]
        public class CompletedEvaluatorHandlers : Name<ISet<IObserver<ICompletedEvaluator>>>
        {
        }

        [NamedParameter(documentation: "Called when an allocated evaluator is given to the client.", defaultClasses: new[] { typeof(DefaultEvaluatorAllocationHandler) })]
        public class AllocatedEvaluatorHandlers : Name<ISet<IObserver<IAllocatedEvaluator>>>
        {
        }

        [NamedParameter(documentation: "Running task handler.", defaultClasses: new[] { typeof(DefaultTaskRunningHandler) })]
        public class RunningTaskHandlers : Name<ISet<IObserver<IRunningTask>>>
        {
        }

        [NamedParameter(documentation: "Running task during driver restart handler.", defaultClasses: new[] { typeof(DefaultDriverRestartTaskRunningHandler) })]
        public class DriverRestartRunningTaskHandlers : Name<ISet<IObserver<IRunningTask>>>
        {
        }

        [NamedParameter(documentation: "Task exception handler.", defaultClasses: new[] { typeof(DefaultTaskFailureHandler) })]
        public class FailedTaskHandlers : Name<ISet<IObserver<IFailedTask>>>
        {
        }

        [NamedParameter(documentation: "Task message handler.", defaultClasses: new[] { typeof(DefaultTaskMessageHandler) })]
        public class TaskMessageHandlers : Name<ISet<IObserver<ITaskMessage>>>
        {
        }

        [NamedParameter(documentation: "Http Event Handlers.", defaultClasses: new[] { typeof(DefaultHttpHandler) })]
        public class HttpEventHandlers : Name<ISet<IHttpHandler>>
        {
        }

        [NamedParameter(documentation: "Completed task handler.", defaultClasses: new[] { typeof(DefaultTaskCompletionHandler) })]
        public class CompletedTaskHandlers : Name<ISet<IObserver<ICompletedTask>>>
        {
        }

        [NamedParameter(documentation: "Suspended task handler.", defaultClasses: new[] { typeof(DefaultTaskSuspensionHandler) })]
        public class SuspendedTaskHandlers : Name<ISet<IObserver<ISuspendedTask>>>
        {
        }

        [NamedParameter(documentation: "Handler for IActiveContext.", defaultClasses: new[] { typeof(DefaultContextActiveHandler) })]
        public class ActiveContextHandlers : Name<ISet<IObserver<IActiveContext>>>
        {
        }

        [NamedParameter(documentation: "Handler for IActiveContext received during driver restart.", defaultClasses: new[] { typeof(DefaultDriverRestartContextActiveHandler) })]
        public class DriverRestartActiveContextHandlers : Name<ISet<IObserver<IActiveContext>>>
        {
        }

        [NamedParameter(documentation: "Handler for ClosedContext.", defaultClasses: new[] { typeof(DefaultContextClosureHandler) })]
        public class ClosedContextHandlers : Name<ISet<IObserver<IClosedContext>>>
        {
        }

        [NamedParameter(documentation: "Handler for FailedContext.", defaultClasses: new[] { typeof(DefaultContextFailureHandler) })]
        public class FailedContextHandlers : Name<ISet<IObserver<IFailedContext>>>
        {
        }

        [NamedParameter(documentation: "Handler for ContextMessage.", defaultClasses: new[] { typeof(DefaultContextMessageHandler) })]
        public class ContextMessageHandlers : Name<ISet<IObserver<IContextMessage>>>
        {
        }

        [NamedParameter("Command Line Arguments supplied by client", "CommandLineArguments", null)]
        public class ArgumentSets : Name<ISet<string>>
        {
        }

        [NamedParameter("Additional trace listners supplied by client", "TraceListeners", null, defaultClasses: new[] { typeof(DefaultCustomTraceListener) })]
        public class TraceListenersSet : Name<ISet<TraceListener>>
        {
        }

        [NamedParameter("Custom Trace Level", "TraceLevel", defaultValue: _verboseLevel)]
        public class TraceLevel : Name<string>
        {
        }

        //[NamedParameter(documentation: "Job message handler.", defaultClasses: new[] { typeof(DefaultClientMessageHandler) })]
        //public class ClientMessageHandlers : Name<ISet<IObserver<byte[]>>>
        //{
        //}

        //[NamedParameter(documentation: "Client close handler.", defaultClasses: new[] { typeof(DefaultClientCloseHandler) })]
        //public class ClientCloseHandlers : Name<ISet<IObserver<byte[]>>>
        //{
        //}
    }
}
