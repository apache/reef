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
    /// if cancellation signal is detected, the manager creates JobCancelled event and propagates the event to all subscriber.
    /// the manager is used by IMRU driver to enable job cancellation based on job definition.
    /// </summary>
    internal sealed class JobLifeCycleManager :
        IDisposable,
        IJobLifecycleManager,
        IObserver<IDriverStarted>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(JobLifeCycleManager));

        private Timer timer;
        private int timerIntervalSec = 1000;
        private IJobCancelledDetector CancellationDetector { get; set; }
        private object lockDispose = new object();
        private readonly List<IObserver<IJobCancelled>> _observers = new List<IObserver<IJobCancelled>>();

        [Inject]
        private JobLifeCycleManager(
            IJobCancelledDetector cancelletionDetector,
            [Parameter(typeof(SleepIntervalParameter))] int sleepIntervalSec)
        {
            this.CancellationDetector = cancelletionDetector;
            this.timerIntervalSec = sleepIntervalSec;
        }

        ~JobLifeCycleManager()
        {
            Dispose();
        }

        public void OnNext(IDriverStarted value)
        {
            if (this.CancellationDetector == null
                || this.CancellationDetector is JobCancellationDetectorAlwaysFalse
                || timerIntervalSec <= 0)
            {
                Logger.Log(Level.Info, "Cancellation detector is null or default or timer internval is negative - no need to start Timer for job lifecycle manager. CancellationDetector: {0}, timer interval: {1}", CancellationDetector, timerIntervalSec);
                return;
            }

            Logger.Log(Level.Info, "OnDriverStart: starting timer to monitor job status. timer interval: {0}, cancellation detector: {1}", timerIntervalSec, CancellationDetector);

            // start timer to monitor cancellation signal
            timer = new Timer(timerIntervalSec * 1000);
            timer.Elapsed += OnTimer;
            timer.AutoReset = true;
            timer.Start();
        }

        public IDisposable Subscribe(IObserver<IJobCancelled> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException("observer");
            }

            lock (_observers)
            {
                if (!_observers.Contains(observer))
                {
                    _observers.Add(observer);
                }
            }

            return new AnonymousDisposable(() => Unsubscribe(observer));
        }

        public void Dispose()
        {
            if (this.timer != null)
            {
                lock (lockDispose)
                {
                    if (timer != null)
                    {
                        timer.Stop();
                        timer.Dispose();
                        timer = null;
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
            lock (_observers)
            {
                _observers.Remove(observer);
            }
        }

        private void OnTimer(object source, ElapsedEventArgs e)
        {
            if (!_observers.Any())
            {
                Logger.Log(Level.Warning, "There are no observers for cancellation event: skipping cancellation detection");
                return;
            }

            string cancellationMessage = null;
            if (IsJobCancelled(out cancellationMessage))
            {
                Logger.Log(Level.Info, "Detected Job cancellation ({0}): sending JobCancelled event to observers: {1}", cancellationMessage, _observers);
                var cancelEvent = new JobCancelled(DateTime.Now, cancellationMessage);

                lock (_observers)
                {
                    _observers.ForEach(o => o.OnNext(cancelEvent));
                }
            }
        }

        private bool IsJobCancelled(out string cancellationMessage)
        {
            var isCancelled = false;
            cancellationMessage = null;

            try
            {
                isCancelled = CancellationDetector != null && CancellationDetector.IsJobCancelled(out cancellationMessage);
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
                this.DisposeAction = disposeAction;
            }

            public void Dispose()
            {
                DisposeAction();
            }
        }
    }
}