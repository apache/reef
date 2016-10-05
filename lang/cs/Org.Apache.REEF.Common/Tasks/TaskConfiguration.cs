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
using System.Diagnostics.CodeAnalysis;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;

[module: SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1401:FieldsMustBePrivate", Justification = "static readonly field, typical usage in configurations")]

namespace Org.Apache.REEF.Common.Tasks
{
    public sealed class TaskConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        ///  The identifier of the task.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly RequiredParameter<string> Identifier = new RequiredParameter<string>();

        /// <summary>
        /// The task to instantiate.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly RequiredImpl<ITask> Task = new RequiredImpl<ITask>();

        /// <summary>
        /// for task suspension. Defaults to task failure if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<ISuspendEvent>> OnSuspend = new OptionalImpl<IObserver<ISuspendEvent>>();

        /// <summary>
        /// for messages from the driver. Defaults to task failure if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IDriverMessageHandler> OnMessage = new OptionalImpl<IDriverMessageHandler>();

        /// <summary>
        /// for heartbeat status changes from the Driver. Does not do anything if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IDriverConnectionMessageHandler> OnDriverConnectionChanged = new OptionalImpl<IDriverConnectionMessageHandler>();

        /// <summary>
        /// for closure requests from the driver. Defaults to task failure if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<ICloseEvent>> OnClose = new OptionalImpl<IObserver<ICloseEvent>>();

        /// <summary>
        /// Message source invoked upon each evaluator heartbeat.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<ITaskMessageSource> OnSendMessage = new OptionalImpl<ITaskMessageSource>();

        /// <summary>
        /// to receive TaskStart after the Task.call() method was called.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<ITaskStart>> OnTaskStart = new OptionalImpl<IObserver<ITaskStart>>();

        /// <summary>
        /// to receive TaskStop after the Task.call() method returned.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<ITaskStop>> OnTaskStop = new OptionalImpl<IObserver<ITaskStop>>();

        /// <summary>
        /// The memento to be passed to Task.call().
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalParameter<string> Memento = new OptionalParameter<string>();

        public static ConfigurationModule ConfigurationModule
        {
            get
            {
                return new TaskConfiguration()
                    .BindImplementation(GenericType<ITask>.Class, Task)
                    .BindSetEntry(GenericType<TaskConfigurationOptions.TaskMessageSources>.Class, OnSendMessage)
                    .BindImplementation(GenericType<IDriverMessageHandler>.Class, OnMessage)
                    .BindImplementation(GenericType<IDriverConnectionMessageHandler>.Class, OnDriverConnectionChanged)
                    .BindNamedParameter(GenericType<TaskConfigurationOptions.Identifier>.Class, Identifier)
                    .BindNamedParameter(GenericType<TaskConfigurationOptions.Memento>.Class, Memento)
                    .BindNamedParameter(GenericType<TaskConfigurationOptions.CloseHandler>.Class, OnClose)
                    .BindNamedParameter(GenericType<TaskConfigurationOptions.SuspendHandler>.Class, OnSuspend)
                    .BindSetEntry(GenericType<TaskConfigurationOptions.StartHandlers>.Class, OnTaskStart)
                    .BindSetEntry(GenericType<TaskConfigurationOptions.StopHandlers>.Class, OnTaskStop)
                    .Build();
            }
        }
    }
}
