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
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Logging;
using Org.Apache.REEF.IMRU.OnREEF.Parameters;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// JobLifecycleManager orchestrates job cancellation flow.
    /// If job cancellation detector is configured in job definition, the manager starts a timer and periodically checks for cancellation signal.
    /// if cancellation signal is detected, the manager creates JobCancelled event and propagates the event to all subscribers.
    /// the manager is used by IMRU driver to enable job cancellation based on the jobCancellationConfiguration in job definition.
    /// </summary>
    internal sealed class JobLifeCycleManager :
        IDisposable,
        IJobLifecycleManager
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(JobLifeCycleManager));

        private Timer _timer;
        private readonly int _timerIntervalSec;
        private readonly IJobCancelledDetector _cancellationDetector;
        private readonly object _disposeLock = new object();
        private readonly List<IObserver<IJobCancelled>> _observers = new List<IObserver<IJobCancelled>>();

        [Inject]
        private JobLifeCycleManager(
            IJobCancelledDetector cancelletionDetector,
            [Parameter(typeof(SleepIntervalParameter))] int sleepIntervalSec)
        {
            _cancellationDetector = cancelletionDetector;
            _timerIntervalSec = sleepIntervalSec;
            InitTimer();
        }

        ~JobLifeCycleManager()
        {
            Dispose();
        }

        private void InitTimer()
        {
            if (_cancellationDetector == null)
            {
                Logger.Log(Level.Info, "Cancellation detector is null - no need to start Timer for job lifecycle manager");
                return;
            }

            if (_cancellationDetector is JobCancellationDetectorAlwaysFalse)
            {
                Logger.Log(Level.Info, "Cancellation detector is default - no need to start Timer for job lifecycle manager.");
                return;
            }

            if (_timerIntervalSec <= 0)
            {
                Logger.Log(Level.Info, "Timer interval ({0}) is not positive - can't start Timer for job lifecycle manager.", _timerIntervalSec);
                return;
            }
    
            Logger.Log(Level.Info, "initializing timer to monitor job status. _timer interval: {0}, cancellation detector: {1}", _timerIntervalSec, _cancellationDetector);

            // start timer to monitor cancellation signal
            _timer = new Timer(_timerIntervalSec * 1000);
            _timer.Elapsed += OnTimer;
            _timer.AutoReset = true;
        }

        public IDisposable Subscribe(IObserver<IJobCancelled> observer)
        {
            Logger.Log(Level.Info, "Adding subscriber: {0}", observer);

            if (observer == null)
            {
                throw new ArgumentNullException("observer");
            }

            lock (_observers)
            {
                if (!_observers.Contains(observer))
                {
                    _observers.Add(observer);
                    EnsureTimerStarted();
                }
            }

            return new AnonymousDisposable(() => Unsubscribe(observer));
        }

        public void Dispose()
        {
            if (_timer != null)
            {
                lock (_disposeLock)
                {
                    if (_timer != null)
                    {
                        _timer.Stop();
                        _timer.Dispose();
                        _timer = null;
                    }
                }
            }
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }

        private void Unsubscribe(IObserver<IJobCancelled> observer)
        {
            Logger.Log(Level.Info, "Removing subscriber: {0}", observer);

            lock (_observers)
            {
                _observers.Remove(observer);
                if (!_observers.Any())
                {
                    EnsureTimerStopped();
                }
            }
        }

        private void EnsureTimerStarted()
        {
            Logger.Log(Level.Info, "Ensure Timer STARTED. Current timer enabled state: {0}", GetTimerEnabledState());

            // _timer can be null if initialization detected it will not work as expected - corresponding logs created
            // or after dispose, in both case ignore the timer start.
            if (_timer != null && !_timer.Enabled)
            {
                _timer.Start();
                Logger.Log(Level.Info, "Timer started");
            }
        }

        private string GetTimerEnabledState()
        {
            return _timer == null ? "timer is null" : _timer.Enabled.ToString();
        }

        private void EnsureTimerStopped()
        {
            Logger.Log(Level.Info, "Ensure Timer STOPPED. Current timer enabled state: {0}", GetTimerEnabledState());

            if (_timer != null)
            {
                _timer.Stop();
                Logger.Log(Level.Info, "Timer stopped");
            }
        }

        private void OnTimer(object source, ElapsedEventArgs e)
        {
            lock (_observers)
            {
                if (!_observers.Any())
                {
                    Logger.Log(Level.Info,
                        "There are no observers for cancellation event: skipping cancellation detection");
                    return;
                }
            }

            string cancellationMessage = null;
            if (IsJobCancelled(out cancellationMessage))
            {
                Logger.Log(
                    Level.Info, 
                    "Detected Job cancellation ({0}): sending JobCancelled event to observers: {1}", 
                    cancellationMessage, 
                    ToCsvString(_observers));

                var cancelEvent = new JobCancelled(DateTime.Now, cancellationMessage);

                lock (_observers)
                {
                    _observers.ForEach(o => o.OnNext(cancelEvent));
                }
            }
        }

        private static string ToCsvString<T>(IEnumerable<T> list)
        {
            if (list == null)
            {
                return "null";
            }

            return string.Join(",", list.Take(10).Select(m => m == null ? "null" : m.ToString()));
        }

        private bool IsJobCancelled(out string cancellationMessage)
        {
            var isCancelled = false;
            cancellationMessage = null;

            try
            {
                isCancelled = _cancellationDetector != null && _cancellationDetector.IsJobCancelled(out cancellationMessage);
            }
            catch (Exception ex)
            {
                Logger.Log(Level.Error, "IsCancelled check failed. Exception:{0}", ex);
                isCancelled = false;
            }

            return isCancelled;
        }

        private class AnonymousDisposable : IDisposable
        {
            private Action DisposeAction { get; set; }

            public AnonymousDisposable(Action disposeAction)
            {
                DisposeAction = disposeAction;
            }

            public void Dispose()
            {
                DisposeAction();
            }
        }
    }
}