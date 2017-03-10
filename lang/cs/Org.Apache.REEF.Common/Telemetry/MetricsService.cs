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
        private readonly IDictionary<string, ICounter> _counters = new ConcurrentDictionary<string, ICounter>();

        /// <summary>
        /// It can be bound with driver configuration as a context message handler
        /// </summary>
        [Inject]
        private MetricsService()
        {
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
                }
                else
                {
                    _counters.Add(counter.Name, counter);
                }

                Logger.Log(Level.Verbose, "Counter name: {0}, value: {1}, description: {2}, time: {3}.", counter.Name, counter.Value, counter.Description, new DateTime(counter.Timestamp));
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