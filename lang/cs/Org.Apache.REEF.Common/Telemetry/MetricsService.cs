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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
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
        /// Registration of counters
        /// </summary>
        private readonly IDictionary<string, ICounter> _counters = new ConcurrentDictionary<string, ICounter>();

        /// <summary>
        /// Total increment since last sink for all the counters
        /// </summary>
        private readonly IDictionary<string, int> _totalIncrementSinceLastSink = new ConcurrentDictionary<string, int>();

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
            [Parameter(typeof(CounterSinkThreshold))] int counterSinkThreshold)
        {
            _metricsSinks = metricsSinks;
            _counterSinkThreshold = counterSinkThreshold;
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

            foreach (var counter in counters.GetCounters())
            {
                ICounter c;
                if (_counters.TryGetValue(counter.Name, out c))
                {
                    //// TODO: [REEF-1748] The following cases need to be considered in determine how to update the counter:
                    //// if evaluator contains the aggregated values, the value will override existing value
                    //// if evaluator only keep delta, the value should be added at here. But the value in the evaluator should be reset after message is sent
                    //// For the counters from multiple evaluators with the same counter name, the value should be aggregated here
                    //// We also need to consider failure cases.  
                    _counters[counter.Name] = counter;

                    if (_totalIncrementSinceLastSink.ContainsKey(counter.Name))
                    {
                        _totalIncrementSinceLastSink[counter.Name] += counter.Value - c.Value;
                    }
                    else
                    {
                        _totalIncrementSinceLastSink.Add(counter.Name, counter.Value - c.Value);
                    }
                }
                else
                {
                    _counters.Add(counter.Name, counter);
                    _totalIncrementSinceLastSink.Add(counter.Name, counter.Value);
                }

                Logger.Log(Level.Verbose, "Counter name: {0}, value: {1}, description: {2}, time: {3},  incrementSinceLastSink: {4}.", 
                    counter.Name, counter.Value, counter.Description, new DateTime(counter.Timestamp), _totalIncrementSinceLastSink[counter.Name]);
            }

            if (TriggerSink())
            {
                SinkCounters();
                _totalIncrementSinceLastSink.Clear();
            }
        }

        /// <summary>
        /// The condition that triggers the sink. The condition can be modified later.
        /// </summary>
        /// <returns></returns>
        private bool TriggerSink()
        {
            return _totalIncrementSinceLastSink.Sum(e => e.Value) > _counterSinkThreshold;
        }

        /// <summary>
        /// Preparing data and call Sink 
        /// </summary>
        private void SinkCounters()
        {
            var set = new HashSet<KeyValuePair<string, string>>();
            foreach (var c in _counters)
            {
                set.Add(new KeyValuePair<string, string>(c.Key, c.Value.Value.ToString()));
            }

            Sink(set);
        }

        /// <summary>
        /// Call each Sink to sink the data in the counters
        /// </summary>
        private void Sink(ISet<KeyValuePair<string, string>> set)
        {
            foreach (var s in _metricsSinks)
            {
                s.Sink(set);
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