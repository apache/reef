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
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Context
{
    /// <summary>
    /// This class is used to trigger all the context life-cycle dependent events.
    /// </summary>
    internal sealed class ContextLifeCycle
    {
        private readonly ISet<IObserver<IContextStart>> _contextStartHandlers;
        private readonly ISet<IObserver<IContextStop>> _contextStopHandlers;
        private readonly ISet<IContextMessageSource> _contextMessageSources;
        private readonly ISet<IContextMessageHandler> _contextMessageHandlers;

        [Inject]
        private ContextLifeCycle(
            [Parameter(typeof(ContextConfigurationOptions.ContextIdentifier))] string contextId,
            [Parameter(typeof(ContextConfigurationOptions.StartHandlers))] ISet<IObserver<IContextStart>> contextStartHandlers,
            [Parameter(typeof(ContextConfigurationOptions.StopHandlers))] ISet<IObserver<IContextStop>> contextStopHandlers,
            [Parameter(typeof(ContextConfigurationOptions.ContextMessageSources))] ISet<IContextMessageSource> contextMessageSources,
            [Parameter(typeof(ContextConfigurationOptions.ContextMessageHandlers))] ISet<IContextMessageHandler> contextMessageHandlers)
        {
            Id = contextId;
            _contextStartHandlers = contextStartHandlers;
            _contextStopHandlers = contextStopHandlers;
            _contextMessageSources = contextMessageSources;
            _contextMessageHandlers = contextMessageHandlers;
        }

        public string Id { get; private set; }

        public ISet<IContextMessageSource> ContextMessageSources
        {
            get { return _contextMessageSources; }
        }

        /// <summary>
        /// Fires ContextStart to all registered event handlers.
        /// </summary>
        public void Start()
        {
            IContextStart contextStart = new ContextStartImpl(Id);

            foreach (var startHandler in _contextStartHandlers)
            {
                startHandler.OnNext(contextStart);
            }
        }

        /// <summary>
        /// Fires ContextStop to all registered event handlers.
        /// </summary>
        public void Close()
        {
            IContextStop contextStop = new ContextStopImpl(Id);
            foreach (var stopHandler in _contextStopHandlers)
            {
                stopHandler.OnNext(contextStop);
            }
        }

        public void HandleContextMessage(byte[] message)
        {
            foreach (var contextMessageHandler in _contextMessageHandlers)
            {
                contextMessageHandler.OnNext(message);
            }
        }
    }
}
