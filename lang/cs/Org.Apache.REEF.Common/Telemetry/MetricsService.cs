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
using System.Linq;
using System.Threading.Tasks;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities;
using Org.Apache.REEF.Utilities.Attributes;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Telemetry
{
    /// <summary>
    /// Metrics service. It is also a context message handler.
    /// </summary>
    [Unstable("0.16", "This is a simple MetricsService. More functionalities will be added.")]
    internal sealed class MetricsService : IObserver<IContextMessage>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MetricsService));

        /// <summary>
        /// Contains Counters received in the Metrics service
        /// </summary>
        private readonly CountersData _countersData;

        /// <summary>
        /// A set of metrics sinks
        /// </summary>
        private readonly ISet<IMetricsSink> _metricsSinks;

        /// <summary>
        /// The threshold that triggers the sinks. 
        /// Currently only one threshold is defined for all the counters. Later, it can be extended to define a threshold per counter.
        /// </summary>
        private readonly int _counterSinkThreshold;

        /// <summary>
        /// It can be bound with driver configuration as a context message handler
        /// </summary>
        [Inject]
        private MetricsService(
            [Parameter(typeof(MetricSinks))] ISet<IMetricsSink> metricsSinks,
            [Parameter(typeof(CounterSinkThreshold))] int counterSinkThreshold,
            CountersData countersData)
        {
            _metricsSinks = metricsSinks;
            _counterSinkThreshold = counterSinkThreshold;
            _countersData = countersData;
        }

        /// <summary>
        /// It is called whenever context message is received
        /// </summary>
        /// <param name="contextMessage">Serialized EvaluatorMetrics</param>
        public void OnNext(IContextMessage contextMessage)
        {
            var msgReceived = ByteUtilities.ByteArraysToString(contextMessage.Message);
            var counters = new EvaluatorMetrics(msgReceived).GetMetricsCounters();
            Logger.Log(Level.Info, "Received {0} counters with context message: {1}.", counters.GetCounters().Count(), msgReceived);

            _countersData.Update(counters);

            if (_countersData.TriggerSink(_counterSinkThreshold))
            {
                Sink(_countersData.GetCounterData());
                _countersData.Reset();
            }
        }

        /// <summary>
        /// Call each Sink to sink the data in the counters
        /// </summary>
        private void Sink(ISet<KeyValuePair<string, string>> set)
        {
            foreach (var s in _metricsSinks)
            {
                try
                {
                    Task.Run(() => s.Sink(set));
                }
                catch (Exception e)
                {
                    Logger.Log(Level.Error, "Exception happens during the sink for Sink {0} with Exception: {1}.", s.GetType().AssemblyQualifiedName, e);
                }
                finally
                {
                    s.Dispose();
                }
            }
        }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }
    }
}