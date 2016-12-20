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
using System.Timers;
using Org.Apache.REEF.Common.Metrics.Api;
using Org.Apache.REEF.Common.Metrics.MetricsSystem.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Metrics.MetricsSystem
{
    /// <summary>
    /// Default impelementation of <see cref="IMetricsSystem"/>. This class orchestrates the 
    /// whole flow of metrics from sources to consumers/sinks. Snapshot is taken either at 
    /// fixed configurable intervals or by an immediate request from the user via 
    /// <see cref="PublishMetricsNow"/>. This is a thread-safe class.
    /// </summary>
    internal sealed class MetricsSystem : IMetricsSystem
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(MetricsSystem));

        private readonly Dictionary<string, MetricsSourceHandler> _sources =
            new Dictionary<string, MetricsSourceHandler>();

        private readonly Dictionary<int, SinkHandler> _sinks = new Dictionary<int, SinkHandler>();
        private readonly SinkHandlerParameters _sinkParameters;
        private int _currentSinkCount = 0;
        private readonly Timer _timer;
        private readonly IMetricsCollectorMutable _metricsCollector;
        private readonly IList<SourceSnapshot> _sourceSnapshots = new List<SourceSnapshot>();
        private bool _shutDown = false;
        private readonly object _lock = new object();
        private readonly IMetricsFilter _sourceFilter;

        /// <summary>
        /// Injectable constructor
        /// </summary>
        /// <param name="sinkParameters">Parameters for creating sink handler.</param>
        /// <param name="periodicTimerInMs">Periodic time after which snapshot will be taken.</param>
        /// <param name="getUnchangedMetrics">Whether to get even unchanged metrics from the sources.</param>
        /// <param name="metricsCollector">The metrics collector used to take snapshot.</param>
        /// <param name="sourceFilter">Filter that allows metrics system to skip some particular sources.</param>
        [Inject]
        private MetricsSystem(SinkHandlerParameters sinkParameters,
            [Parameter(typeof(MetricsSystemPeriodicTimer))] int periodicTimerInMs,
            [Parameter(typeof(MetricsSystemGetUnchangedMetrics))] bool getUnchangedMetrics,
            IMetricsCollectorMutable metricsCollector,
            IMetricsFilter sourceFilter)
        {
            _sinkParameters = sinkParameters;
            _timer = new Timer(periodicTimerInMs);
            _timer.Elapsed += (sender, e) => PublishMetrics(getUnchangedMetrics);
            _metricsCollector = metricsCollector;
            _sourceFilter = sourceFilter;
        }

        /// <summary>
        /// Subscribes a new consumer/sink with the system. Throws exception if sink 
        /// is already registered or system is in shutdown mode. 
        /// </summary>
        /// <param name="observer">The consumer/sink.</param>
        /// <returns>A reference to interface that allows sink to stop receiving metrics.</returns>
        public IDisposable Subscribe(IObserver<IMetricsRecord> observer)
        {
            lock (_lock)
            {
                if (!_shutDown)
                {
                    bool isPresent = _sinks.Any(x => x.Value.Sink == observer);
                    if (!isPresent)
                    {
                        _currentSinkCount++;
                        _sinks[_currentSinkCount] = new SinkHandler(_sinkParameters, observer);
                        return new SinkDisposableHandler(_sinks, _currentSinkCount, _lock);
                    }
                    throw new MetricsException(
                        "Trying to subscribe already registered metrics record observer.");
                }
                throw new MetricsException(
                    "Trying to subscribe metrics record observer while metrics system is shutting down");
            }
        }

        /// <summary>
        /// Starts the metrics system. The underlying sink handlers throw exception if we start them again.
        /// </summary>
        public void Start()
        {
            lock (_lock)
            {
                if (_shutDown)
                {
                    throw new MetricsException("Metrics system is in shut down mode. So aborting the start.");
                }
                _timer.Start();
                foreach (var sink in _sinks)
                {
                    sink.Value.Start();
                }
            }
        }

        /// <summary>
        /// Stops the metrics system. The underlying sink handlers throw exception if we stop again.
        /// Call ShutDown() if you are done with MetricsSystem.
        /// </summary>
        public void Stop()
        {
            lock (_lock)
            {
                if (_shutDown)
                {
                    throw new MetricsException("Metrics system is in shut down mode. So aborting the stop.");
                }
                _timer.Stop();
                foreach (var sink in _sinks)
                {
                    sink.Value.Stop();
                }
            }
        }

        /// <summary>
        /// Registers a source with the metrics system.
        /// </summary>
        /// <param name="name">Name of the source.</param>
        /// <param name="desc">Description of the source.</param>
        /// <param name="source">The underlying source.</param>
        /// <returns>Reference to the added source.</returns>
        public IMetricsSource RegisterSource(string name, string desc, IMetricsSource source)
        {
            lock (_lock)
            {
                if (_shutDown)
                {
                    Logger.Log(Level.Warning,
                        "Metrics system is in shutdown mode. So aborting registering the metrics source");
                    return null;
                }
                if (!_sources.ContainsKey(name))
                {
                    _sources[name] = new MetricsSourceHandler(name, source);
                    Logger.Log(Level.Info,
                        string.Format("Adding source with name: {0} and description: {1}", name, desc));
                }
                else
                {
                    Logger.Log(Level.Info,
                        string.Format("Source with name: {0} already exists. Returning already existing source", name));
                }
                return source;
            }
        }

        /// <summary>
        /// Unregisters a source by name. Does not throw exception if source does not exist.
        /// </summary>
        /// <param name="name">Name of the source.</param>
        public void UnRegisterSource(string name)
        {
            lock (_lock)
            {
                if (_shutDown)
                {
                    return;
                }
                if (!_sources.Remove(name))
                {
                    string message =
                        string.Format(
                            "Unable to unregister source with name: {0}. Either it does not exist or some error happened",
                            name);
                    Logger.Log(Level.Info, message);
                }
            }
        }

        /// <summary>
        /// Returns source by name. Null if it does not exist.
        /// </summary>
        /// <param name="name">Name of the source.</param>
        public IMetricsSource GetSource(string name)
        {
            lock (_lock)
            {
                return _sources.ContainsKey(name) ? _sources[name].Source : null;
            }
        }

        /// <summary>
        /// Asks the system to take a snapshot from the source immediately and push it to 
        /// sink handler.
        /// </summary>
        /// <param name="all">Whether to also publish metrics that did not change.</param>
        public void PublishMetricsNow(bool all)
        {
            lock (_lock)
            {
                if (_shutDown)
                {
                    return;
                }
                PublishMetrics(all);
            }
        }

        /// <summary>
        /// Shuts down the metrics system. Call Stop() befor ShutDown()
        /// </summary>
        /// <returns>True if shutdown is successful.</returns>
        public bool ShutDown()
        {
            lock (_lock)
            {
                _shutDown = true;
                foreach (var sink in _sinks)
                {
                    sink.Value.ShutDown();
                }
                _sinks.Clear();
                _sources.Clear();
                return true;
            }
        }

        /// <summary>
        /// Function called to take snapshot from sources and put the in the sink queue. 
        /// This method is either called periodically or by user via. <see cref="PublishMetricsNow"/>.
        /// </summary>
        private void PublishMetrics(bool all)
        {
            lock (_lock)
            {
                if (_sinks.Count != 0 && _sources.Count != 0)
                {
                    _sourceSnapshots.Clear();
                    foreach (var source in _sources)
                    {
                        if (_sourceFilter.AcceptsName(source.Key))
                        {
                            _metricsCollector.Clear();
                            _sourceSnapshots.Add(source.Value.GetMetrics(_metricsCollector, all));
                        }
                    }
                    foreach (var sink in _sinks)
                    {
                        sink.Value.PutMetricsInQueue(_sourceSnapshots);
                    }
                }
            }
        }

        /// <summary>
        /// Disposable class for IObservable interface. Allows sinks to unregister themselves from 
        /// the metrics system.
        /// </summary>
        private class SinkDisposableHandler : IDisposable
        {
            private readonly Dictionary<int, SinkHandler> _sinkHandlers;
            private readonly int _key;
            private readonly object _lock;

            public SinkDisposableHandler(Dictionary<int, SinkHandler> sinkHandlers, int key, object lockObj)
            {
                _sinkHandlers = sinkHandlers;
                _key = key;
                _lock = lockObj;
            }

            public void Dispose()
            {
                lock (_lock)
                {
                    if (_sinkHandlers.ContainsKey(_key))
                    {
                        if (!_sinkHandlers[_key].IsStopped && !_sinkHandlers[_key].IsShutDown)
                        {
                            _sinkHandlers[_key].Stop();
                        }
                        _sinkHandlers.Remove(_key);
                    }
                    else
                    {
                        Logger.Log(Level.Warning, "Sink to remove from metrics system is not present");
                    }
                }
            }
        }
    }
}