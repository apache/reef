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
using System.Threading;
using System.Threading.Tasks;
using Org.Apache.REEF.Common.Metrics.Api;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.Common.Metrics.MetricsSystem
{
    /// <summary>
    /// Helper class for managing metric consumers or sinks. Since sinks can potentially take 
    /// lot of time consuming metrics (for example, in case metrics care transmitted to driver or other 
    /// remote location), we maintian a queue that maintains snapshot of metrics from sources at different 
    /// times. The queue has a configurable maximum capacity. The consumer asynchronously consumes metrics from 
    /// the queue. What to do exactly if metrics cannot be added 
    /// due to full queue is left for metrics system to handle. Default MetricsSystem simply starts dropping metrics. 
    /// It is assumed that consumers/sinks can fail intermittently. This can happen for example if consumer is 
    /// some web-server or client and hads heavy load in which case some attempts to consume the metrics can fail. 
    /// Hence, an exponential back-off retry strategy is provided to tolerate some intermittent failures.
    /// The class is not thread safe and caller is supposed to maintain the safety.
    /// </summary>
    internal sealed class SinkHandler
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(SinkHandler));
        private readonly SinkHandlerParameters _parameters;
        private CancellationTokenSource _cancellationSource;
        private readonly IObserver<IMetricsRecord> _sink;
        private readonly BlockingCollection<IEnumerable<SourceSnapshot>> _metricsQueue;
        private bool _shutDown = false;
        private bool _stopped = true;
        private Task _dumpToSink = null;

        /// <summary>
        /// Creates a sink handler.
        /// </summary>
        /// <param name="parameters">Parameters for creating the sink handler.</param>
        /// <param name="recordObserver">Consumer or sink for the metrics snapshot.</param>
        public SinkHandler(SinkHandlerParameters parameters, IObserver<IMetricsRecord> recordObserver)
        {
            _parameters = parameters;
            _sink = recordObserver;
            _metricsQueue = new BlockingCollection<IEnumerable<SourceSnapshot>>(_parameters.QueueCapacity);
            _cancellationSource = new CancellationTokenSource();
            _cancellationSource.Token.ThrowIfCancellationRequested();
        }

        /// <summary>
        /// Underlying consumer/sink.
        /// </summary>
        public IObserver<IMetricsRecord> Sink
        {
            get { return _sink; }
        }

        /// <summary>
        /// Tries to add metric snapshots in the queue.
        /// </summary>
        /// <param name="sourceSnapshot">Enumerable of snapshots from different sources.</param>
        /// <returns>True, is it is added, false otherwise.</returns>
        public bool PutMetricsInQueue(IEnumerable<SourceSnapshot> sourceSnapshot)
        {
            return _metricsQueue.TryAdd(sourceSnapshot);
        }

        /// <summary>
        /// Starts the sink handler. The asynchronous process that keeps listening to the 
        /// queue starts.
        /// </summary>
        public void Start()
        {
            if (!_stopped)
            {
                var exception = new MetricsException("Error in SinkHandler",
                    new InvalidOperationException("Calling start on already started sinkhandler"));
                _sink.OnError(exception);
                throw exception;
            }

            if (_shutDown)
            {
                var exception = new MetricsException("Error in SinkHandler",
                   new InvalidOperationException("Sink handler is in shutdown mode"));
                throw exception;
            }
            _cancellationSource = new CancellationTokenSource();
            _cancellationSource.Token.ThrowIfCancellationRequested();
            _dumpToSink = Task.Run(() => TakeMetricsFromQueue());
            _stopped = false;
        }

        /// <summary>
        /// Stops the sink handler. The asynchronous process that keeps listening to the 
        /// queue is stopped.
        /// </summary>
        public void Stop()
        {
            if (_stopped)
            {
                var exception = new MetricsException("Error in SinkHandler",
                    new InvalidOperationException(
                        "Calling stop when the sink handler is not in start or running state"));
                _sink.OnError(exception);
                throw exception;
            }

            if (_shutDown)
            {
                Logger.Log(Level.Info, "Sink handler is already shut down. So aborting the stop");
            }

            _cancellationSource.Cancel();
            _dumpToSink.Wait();
            _dumpToSink = null;
            _stopped = true;
        }

        /// <summary>
        /// Shuts down the whole sink handler. Calls OnCompleted function 
        /// of the sink after flushing the queue.
        /// </summary>
        public void ShutDown()
        {
            _shutDown = true;
            ConsumeFullQueue();
            _sink.OnCompleted();
        }

        /// <summary>
        /// Specifies whether sink handler is shut down.
        /// </summary>
        public bool IsShutDown
        {
            get { return _shutDown; }
        }

        /// <summary>
        /// Specifies whether Sink handler is stopped.
        /// </summary>
        public bool IsStopped
        {
            get { return _stopped; }
        }

        /// <summary>
        /// Takes metrics from the queue and consumes it. This functions runs 
        /// asynchronously. When operation is cancelled due to call to the <see cref="Stop"/> 
        /// call, the full queue is consumed and then the function exits.
        /// </summary>
        private void TakeMetricsFromQueue()
        {
            while (!_cancellationSource.Token.IsCancellationRequested)
            {
                try
                {
                    var snapshot = _metricsQueue.Take(_cancellationSource.Token);
                    ConsumeSnapshot(snapshot);
                }
                catch (OperationCanceledException)
                {
                    ConsumeFullQueue();
                    Logger.Log(Level.Info, "Cancelling putting metrics in sink.");
                    return;
                }
            }

            ConsumeFullQueue();
            Logger.Log(Level.Info, "Cancelling putting metrics in sink.");
        }

        /// <summary>
        /// Consumes all the entries in the queue.
        /// </summary>
        private void ConsumeFullQueue()
        {
            IEnumerable<SourceSnapshot> snapshot = null;
            while (_metricsQueue.TryTake(out snapshot))
            {
                ConsumeSnapshot(snapshot);
            }
        }

        /// <summary>
        /// Consumes a given snapshot from the sources. It retries OnNext with particular 
        /// Retry policy before throwing exception.
        /// </summary>
        /// <param name="snapshot">Collection of snapshots from various sources.</param>
        private void ConsumeSnapshot(IEnumerable<SourceSnapshot> snapshot)
        {
            int tryCounter = 0;
            try
            {
                _parameters.PolicyForRetry.ExecuteAction(() =>
                {
                    tryCounter++;
                    foreach (var source in snapshot)
                    {
                        foreach (var entry in source.Records)
                        {
                            _sink.OnNext(entry);
                        }
                    }
                });
            }
            catch (Exception e)
            {
                var msg = "Failed to push to sink even after " + tryCounter + " retries.";
                Logger.Log(Level.Error, msg);
                _sink.OnError(e);
                throw;
            }
        }
    }
}
