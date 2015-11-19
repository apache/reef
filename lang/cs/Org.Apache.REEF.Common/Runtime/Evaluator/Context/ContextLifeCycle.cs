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
using System.Collections.Generic;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;

namespace Org.Apache.REEF.Common.Runtime.Evaluator.Context
{
    /// <summary>
    /// This class is used to trigger all the context life-cycle dependent events.
    /// </summary>
    class ContextLifeCycle
    {
        private HashSet<IObserver<IContextStart>> _contextStartHandlers;

        private HashSet<IObserver<IContextStop>> _contextStopHandlers;

        private readonly HashSet<IContextMessageSource> _contextMessageSources;

        // @Inject
        public ContextLifeCycle(
            string id,
            HashSet<IObserver<IContextStart>> contextStartHandlers,
            HashSet<IObserver<IContextStop>> contextStopHandlers,
            HashSet<IContextMessageSource> contextMessageSources)
        {
            Id = id;
            _contextStartHandlers = contextStartHandlers;
            _contextStopHandlers = contextStopHandlers;
            _contextMessageSources = contextMessageSources;
        }

        public ContextLifeCycle(string contextId)
        {
            Id = contextId;
            _contextStartHandlers = new HashSet<IObserver<IContextStart>>();
            _contextStopHandlers = new HashSet<IObserver<IContextStop>>();
            _contextMessageSources = new HashSet<IContextMessageSource>();
        }

        public string Id { get; private set; }

        public HashSet<IContextMessageSource> ContextMessageSources
        {
            get { return _contextMessageSources; }
        }

        /// <summary>
        /// Fires ContextStart to all registered event handlers.
        /// </summary>
        public void Start()
        {
            IContextStart contextStart = new ContextStartImpl(Id);
            
            ////TODO: enable
            ////foreach (IObserver<IContextStart> startHandler in _contextStartHandlers)
            ////{
            ////   startHandler.OnNext(contextStart);
            ////}
        }

        /// <summary>
        /// Fires ContextStop to all registered event handlers.
        /// </summary>
        public void Close()
        {
            ////IContextStop contextStop = new ContextStopImpl(Id);
            ////foreach (IObserver<IContextStop> startHandler in _contextStopHandlers)
            ////{
            ////   startHandler.OnNext(contextStop);
            ////}
        }

        public void HandleContextMessage(byte[] message)
        {
            // contextMessageHandler.onNext(message);
        }

        /// <summary>
        /// get the set of ContextMessageSources configured
        /// </summary>
        /// <returns>(a shallow copy of) the set of ContextMessageSources configured.</returns>
        public HashSet<IContextMessageSource> GetContextMessageSources()
        {
            return new HashSet<IContextMessageSource>(_contextMessageSources);
        }
    }
}
