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
using System.Threading;
using Org.Apache.REEF.Driver;
using Org.Apache.REEF.IMRU.API;
using Org.Apache.REEF.IMRU.OnREEF.Driver;
using Xunit;

namespace Org.Apache.REEF.IMRU.Tests
{
    public class JobLifecycleManagerTest
    {
        [Fact]
        public void JobLifeCyclemanger_SendsJobCancelledEvent()
        {
            string expectedMessage = "cancelled";
            var observer = JobLifeCycleMangerEventTest(
                detector: new DetectorStubStatic(true, expectedMessage))
                .FirstOrDefault();

            AssertCancelEvent(observer, true, expectedMessage);
        }

        [Fact]
        public void JobLifeCyclemanger_SendsJobCancelledEventToMultiplyObservers()
        {
            string expectedMessage = "cancelled";
            var observers = JobLifeCycleMangerEventTest(
                detector: new DetectorStubStatic(true, expectedMessage));

            foreach (var observer in observers)
            {
                AssertCancelEvent(observer, true, expectedMessage);
            }
        }

        [Fact]
        public void JobLifeCyclemanger_ChecksDetectorPeriodically()
        {
            string expectedMessage = "cancelled";
            int isCancelledCheckCounter = 0;

            var observer = JobLifeCycleMangerEventTest(
                detector: new DetectorStubStatic(true, expectedMessage, testAction: () => { isCancelledCheckCounter++; }),
                signalCheckPeriodSec: 1,
                waitForEventPeriodSec: 6)
                .FirstOrDefault();

            Assert.True(isCancelledCheckCounter >= 5, "Expected 5+ IsCancelled checks in 6 sec (check interval = 1 sec). Actual check counter: " + isCancelledCheckCounter);
            AssertCancelEvent(observer, true, expectedMessage);
        }

        [Fact]
        public void JobLifeCyclemanger_NoSignal_DoesNotSendEvent()
        {
            var observer = JobLifeCycleMangerEventTest(
                detector: new DetectorStubStatic(false))
                .FirstOrDefault();

            AssertCancelEvent(observer, false);
        }

        [Fact]
        public void JobLifeCyclemanger_DetectorNull_DoesNotSendEvent()
        {
            var observer = JobLifeCycleMangerEventTest(
                detector: null)
                .FirstOrDefault();

            AssertCancelEvent(observer, false);
        }

        [Fact]
        public void JobLifeCyclemanger_NoObservers_DoesNotCheckForSignal()
        {
            int isCancelledCheckCounter = 0;

            var observer = JobLifeCycleMangerEventTest(
                detector: new DetectorStubStatic(true, "cancelled", testAction: () => { isCancelledCheckCounter++; }),
                subscribeObserver: false,
                signalCheckPeriodSec: 1,
                waitForEventPeriodSec: 6)
                .FirstOrDefault();

            Assert.True(isCancelledCheckCounter == 0, "Expected no checks for cancellation if there are no subscribers. Actual check counter: " + isCancelledCheckCounter);
            AssertCancelEvent(observer, false);
        }

        private IEnumerable<TestObserver> JobLifeCycleMangerEventTest(
            IJobCancelledDetector detector,
            bool subscribeObserver = true,
            int observerCount = 1,
            int signalCheckPeriodSec = 1,
            int waitForEventPeriodSec = 2)
        {
            var manager = new JobLifeCycleManager(detector, signalCheckPeriodSec);
            
            var observers = Enumerable.Range(1, observerCount)
                .Select(_ => subscribeObserver ? new TestObserver(manager) : new TestObserver(null))
                .ToList();

            manager.OnNext(NewStartedEvent());

            Thread.Sleep(waitForEventPeriodSec * 1000);

            return observers;
        }

        private void AssertCancelEvent(TestObserver observer, bool expectedEvent, string expectedMessage = null)
        {
            if (expectedEvent)
            {
                Assert.NotNull(observer.LastEvent);
                Assert.Same(expectedMessage, observer.LastEvent.Message);
            }
            else
            {
                Assert.Null(observer.LastEvent);
            }
        }

        private IDriverStarted NewStartedEvent()
        {
            // event is not really used by the driver, so can use null here
            return null;
        }

        /// <summary>
        /// Test helper class to provide predefined cancel signal for testing
        /// </summary>
        private class DetectorStubStatic : IJobCancelledDetector 
        {
            private bool IsCancelledResponse { get; set; }
            private string CancellationMessage { get; set; }
            private Action ActionOnIsCancelledCall { get; set; }

            public DetectorStubStatic(bool isCancelledResponse, string expectedMessage = null, Action testAction = null)
            {
                IsCancelledResponse = isCancelledResponse;
                CancellationMessage = expectedMessage;
                ActionOnIsCancelledCall = testAction;
            }

            public bool IsJobCancelled(out string cancellationMessage)
            {
                if (ActionOnIsCancelledCall != null)
                {
                    ActionOnIsCancelledCall();
                }

                cancellationMessage = CancellationMessage;
                return IsCancelledResponse;
            }
        }

        /// <summary>
        /// Test helper class to record JobCancelled events from lifecycle manager
        /// </summary>
        private class TestObserver : IObserver<IJobCancelled> 
        {
            public IJobCancelled LastEvent { get; private set; }

            public TestObserver(IObservable<IJobCancelled> eventSource)
            {
                if (eventSource != null)
                {
                    eventSource.Subscribe(this);
                }
            }

            public void OnNext(IJobCancelled value)
            {
                LastEvent = value;
            }

            public void OnError(Exception error)
            {
            }

            public void OnCompleted()
            {
            }
        }
    }
}
