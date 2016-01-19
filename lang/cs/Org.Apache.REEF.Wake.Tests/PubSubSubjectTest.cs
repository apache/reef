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
using System.Reactive;
using System.Threading;
using Org.Apache.REEF.Wake.RX.Impl;
using Xunit;

namespace Org.Apache.REEF.Wake.Tests
{
    public class PubSubSubjectTest
    {
        [Fact]
        public void TestPubSubSubjectSingleThread()
        {
            int sum = 0;

            // Observer that adds sum of numbers up to and including x
            PubSubSubject<int> subject = new PubSubSubject<int>();
            subject.Subscribe(Observer.Create<int>(
                x =>
                {
                    for (int i = 0; i <= x; i++)
                    {
                        sum += i;
                    }
                }));

            subject.OnNext(10);
            subject.OnCompleted();
            Assert.Equal(sum, 55);
        }

        [Fact]
        public void TestPubSubSubjectMultipleThreads()
        {
            int sum = 0;

            PubSubSubject<int> subject = new PubSubSubject<int>();
            subject.Subscribe(Observer.Create<int>(x => sum += x));

            Thread[] threads = new Thread[10];
            for (int i = 0; i < threads.Length; i++)
            {
                threads[i] = new Thread(() =>
                {
                    for (int j = 0; j < 10000; j++)
                    {
                        subject.OnNext(1);
                    }
                });

                threads[i].Start();
            }

            foreach (Thread thread in threads)
            {
                thread.Join();
            }

            Assert.Equal(sum, 100000);
        }

        [Fact]
        public void TestMultipleTypes()
        {
            int sum1 = 0;
            int sum2 = 0;

            PubSubSubject<SuperEvent> subject = new PubSubSubject<SuperEvent>();
            subject.Subscribe(Observer.Create<SubEvent1>(x => sum1 += 100));
            subject.Subscribe(Observer.Create<SubEvent2>(x => sum2 += 500));

            subject.OnNext(new SubEvent1());
            subject.OnNext(new SubEvent2());
            subject.OnNext(new SubEvent2());

            Assert.Equal(sum1, 100);
            Assert.Equal(sum2, 1000);
        }

        [Fact]
        public void TestOnCompleted()
        {
            int sum = 0;

            PubSubSubject<int> subject = new PubSubSubject<int>();
            subject.Subscribe(Observer.Create<int>(x => sum += x));

            subject.OnNext(10);
            Assert.Equal(10, sum);

            subject.OnNext(10);
            Assert.Equal(20, sum);

            // Check that after calling OnCompleted, OnNext will do nothing
            subject.OnCompleted();
            subject.OnNext(10);
            Assert.Equal(20, sum);
        }

        [Fact]
        public void TestOnError()
        {
            int sum = 0;

            PubSubSubject<int> subject = new PubSubSubject<int>();
            subject.Subscribe(Observer.Create<int>(x => sum += x));

            subject.OnNext(10);
            Assert.Equal(10, sum);

            subject.OnNext(10);
            Assert.Equal(20, sum);

            // Check that after calling OnError, OnNext will do nothing
            subject.OnError(new Exception("error"));
            subject.OnNext(10);
            Assert.Equal(20, sum);
        }

        [Fact]
        public void TestDisposeSingleSubject()
        {
            int sum = 0;

            PubSubSubject<int> subject = new PubSubSubject<int>();
            var disposable = subject.Subscribe(Observer.Create<int>(x => sum += x));
            
            subject.OnNext(10);
            subject.OnNext(10);
            subject.OnNext(10);
            Assert.Equal(30, sum);

            // Unregister the subject and check that calling OnNext does nothing
            disposable.Dispose();
            subject.OnNext(10);
            Assert.Equal(30, sum);
        }

        [Fact]
        public void TestDisposeMultipleSubjects()
        {
            int sum1 = 0;
            int sum2 = 0;

            SubEvent1 event1 = new SubEvent1();
            SubEvent2 event2 = new SubEvent2();

            PubSubSubject<SuperEvent> subject = new PubSubSubject<SuperEvent>();
            var disposable1 = subject.Subscribe(Observer.Create<SubEvent1>(x => sum1 += 100));
            var disposable2 = subject.Subscribe(Observer.Create<SubEvent2>(x => sum2 += 500));

            subject.OnNext(event1);
            subject.OnNext(event2);
            subject.OnNext(event2);
            Assert.Equal(sum1, 100);
            Assert.Equal(sum2, 1000);

            // Check that unsubscribing from SubEvent1 does not affect other subscriptions
            disposable1.Dispose();
            subject.OnNext(event1);
            subject.OnNext(event2);
            Assert.Equal(sum1, 100);
            Assert.Equal(sum2, 1500);

            // Unsubscribe from the remaining event types
            disposable2.Dispose();
            subject.OnNext(event1);
            subject.OnNext(event2);
            Assert.Equal(sum1, 100);
            Assert.Equal(sum2, 1500);
        }

        class SuperEvent
        {
        }

        class SubEvent1 : SuperEvent
        {
        }

        class SubEvent2 : SuperEvent
        {
        }
    }
}
