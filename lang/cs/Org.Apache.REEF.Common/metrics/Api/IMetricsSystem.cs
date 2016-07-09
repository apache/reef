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
using Org.Apache.REEF.Utilities.Attributes;

namespace Org.Apache.REEF.Common.Metrics.Api
{
    [Unstable("0.16", "Contract may change.")]
    public interface IMetricsSystem : IObservable<IMetricsRecord>
    {
        /// <summary>
        /// (Re)Start the whole Metrics system. This along with <see cref="Stop"/> can be called 
        /// multiple times to start and stop the metrics system. Exact logic of what to do if 
        /// function is called multiple times before Stop is called is left to the underlying 
        /// implementation.
        /// </summary>
        void Start();

        /// <summary>
        /// Stop the metrics system. The system can be started and stopped again. Exact logic of what to do if 
        /// function is called multiple times before Start is called is left to the underlying 
        /// implementation.
        /// </summary>
        void Stop();

        /// <summary>
        /// Register the metrics source.
        /// </summary>
        /// <param name="name">Name of the source. Must be unique.</param>
        /// <param name="desc">Description of the source.</param>
        /// <param name="source">Metrics source to register.</param>
        /// <returns>Metrics source</returns>
        IMetricsSource RegisterSource(string name, string desc, IMetricsSource source);

        /// <summary>
        /// Unregister the metrics source.
        /// </summary>
        /// <param name="name">Name of the source.</param>
        void UnRegisterSource(string name);

        /// <summary>
        /// Gets the metrics source.
        /// </summary>
        /// <param name="name">Name of the metrics source.</param>
        /// <returns>Metrics source.</returns>
        IMetricsSource GetSource(string name);

        /// <summary>
        /// Requests an immediate publish of all metrics from sources to sinks.
        /// Best effort will be done to push metrics from source to sink synchronously 
        /// before returning. However, if it cannot be done in reasonable time 
        /// it is ok to return to the caller before everything is done. 
        /// </summary>
        void PublishMetricsNow();

        /// <summary>
        /// Completely shuts down the metrics system.
        /// </summary>
        /// <returns>True if proper shutdown happened.</returns>
        bool ShutDown();
    }
}