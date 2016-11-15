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
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Common.Metrics.Api;
using Org.Apache.REEF.Common.Metrics.MetricsSystem.Parameters;
using Org.Apache.REEF.Tang.Annotations;

namespace Org.Apache.REEF.Common.Metrics.MetricsSystem
{
    /// <summary>
    /// Context start handler for metrics system.
    /// </summary>
    internal class MetricsSystemContextStartHandler : IObserver<IContextStart>
    {
        private readonly IMetricsSystem _metricsSystem;

        [Inject]
        private MetricsSystemContextStartHandler(IMetricsSystem metricsSystem,
            IMetricsSource source,
            [Parameter(typeof(ContextSourceParameters.SourceName))] string sourceName,
            [Parameter(typeof(ContextSourceParameters.SourceDesc))] string sourceDesc,
            [Parameter(typeof(ContextSinkParameters.SinkSetName))] ISet<IObserver<IMetricsRecord>> sinks)
        {
            _metricsSystem = metricsSystem;
            _metricsSystem.RegisterSource(sourceName, sourceDesc, source);
            foreach (var sink in sinks)
            {
                _metricsSystem.Subscribe(sink);
            }
        }

        /// <summary>
        /// Gets called when context starts. Metrics system is started.
        /// </summary>
        /// <param name="value">IContextStart object.</param>
        public void OnNext(IContextStart value)
        {
            _metricsSystem.Start();
        }

        /// <summary>
        /// Gets called when there is some error. Metrics system is shutdown.
        /// </summary>
        /// <param name="error">Kind of exception.</param>
        public void OnError(Exception error)
        {
            _metricsSystem.Stop();
            _metricsSystem.ShutDown();
        }

        /// <summary>
        /// Gets called upon context completion. Shuts down the metrics system.
        /// </summary>
        public void OnCompleted()
        {
            _metricsSystem.Stop();
            _metricsSystem.ShutDown();
        }
    }
}
