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

using System;
using System.Diagnostics.CodeAnalysis;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Common.Tasks;
using Org.Apache.REEF.Common.Tasks.Events;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Attributes;

[module: SuppressMessage("StyleCop.CSharp.MaintainabilityRules", "SA1401:FieldsMustBePrivate", Justification = "static field, typical usage in configurations")]

namespace Org.Apache.REEF.Driver.Context
{
    [ClientSide]
    public sealed class ContextConfiguration : ConfigurationModuleBuilder
    {
        /// <summary>
        ///  The identifier of the context.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly RequiredParameter<string> Identifier = new RequiredParameter<string>();

        /// <summary>
        ///  for context start. Defaults to logging if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<IContextStart>> OnContextStart = new OptionalImpl<IObserver<IContextStart>>();

        /// <summary>
        /// for context stop. Defaults to logging if not bound.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<IContextStop>> OnContextStop = new OptionalImpl<IObserver<IContextStop>>();

        /// <summary>
        ///  to be informed right before a Task enters its call() method.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<ITaskStart>> OnTaskStart = new OptionalImpl<IObserver<ITaskStart>>();

        /// <summary>
        ///  to be informed right after a Task exits its call() method.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IObserver<ITaskStop>> OnTaskStop = new OptionalImpl<IObserver<ITaskStop>>();

        /// <summary>
        ///  Source of messages to be called whenever the evaluator is about to make a heartbeat.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IContextMessageSource> OnSendMessage = new OptionalImpl<IContextMessageSource>();

        /// <summary>
        ///   Driver has sent the context a message, and this parameter is used to register a handler on the context for processing that message.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2104:Do not declare read only mutable reference types", Justification = "not applicable")]
        public static readonly OptionalImpl<IContextMessageHandler> OnMessage = new OptionalImpl<IContextMessageHandler>();

        public static ConfigurationModule ConfigurationModule
        {
            get
            {
                return new ContextConfiguration()
                    .BindNamedParameter(GenericType<ContextConfigurationOptions.ContextIdentifier>.Class, Identifier)
                    .BindSetEntry(GenericType<ContextConfigurationOptions.StartHandlers>.Class, OnContextStart)
                    .BindSetEntry(GenericType<ContextConfigurationOptions.StopHandlers>.Class, OnContextStop)
                    .BindSetEntry(GenericType<ContextConfigurationOptions.ContextMessageSources>.Class, OnSendMessage)
                    .BindSetEntry(GenericType<ContextConfigurationOptions.ContextMessageHandlers>.Class, OnMessage)
                    .BindSetEntry(GenericType<TaskConfigurationOptions.StartHandlers>.Class, OnTaskStart)
                    .BindSetEntry(GenericType<TaskConfigurationOptions.StopHandlers>.Class, OnTaskStop)
                    .Build();
            }
        }
    }
}
