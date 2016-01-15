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
using Org.Apache.REEF.Utilities.Collections;
using Xunit;
using Assert = Xunit.Assert;

namespace Org.Apache.REEF.Tests.Utility
{
    public class TestPriorityQueue
    {
        private const int AnyInt = 2;

        [Fact]
        public void PriorityQueueDequeueEmptyQueueThrowsException()
        {
            var q = new PriorityQueue<int>();
            Assert.Equal(0, q.Count);
            Assert.Throws<InvalidOperationException>(() => q.Dequeue());
        }

        [Fact]
        public void PriorityQueuePeekEmptyQueueThrowsException()
        {
            var q = new PriorityQueue<int>();
            Assert.Equal(0, q.Count);
            Assert.Throws<InvalidOperationException>(() => q.Peek());
        }

        [Fact]
        public void PriorityQueueEnqueueOneElementIncrementsCount()
        {
            var q = new PriorityQueue<int>();
            var count = q.Count;
            q.Enqueue(AnyInt);
            Assert.Equal(count + 1, q.Count);
        }

        [Fact]
        public void PriorityQueueDequeueOneElementDecrementsCount()
        {
            var q = new PriorityQueue<int>();
            q.Enqueue(AnyInt);
            var count = q.Count;
            q.Dequeue();
            Assert.Equal(count - 1, q.Count);
        }

        [Fact]
        public void PriorityQueuePeekQueueNoChangeInCount()
        {
            var q = new PriorityQueue<int>();

            q.Enqueue(AnyInt);
            var count = q.Count;
            q.Peek();
            Assert.Equal(count, q.Count);
        }

        [Fact]
        public void PriorityQueueClearEmptiesTheQueue()
        {
            var q = new PriorityQueue<int>();
            q.Enqueue(AnyInt);
            q.Enqueue(AnyInt);
            q.Clear();

            Assert.Equal(0, q.Count);

            Assert.Throws<InvalidOperationException>(() => q.Peek());
        }

        [Fact]
        public void PriorityQueueAddElementsDequeueReturnsThemSortedByPriority()
        {
            var q = new PriorityQueue<int>();
            q.Enqueue(0);
            q.Enqueue(4);
            q.Enqueue(2);
            q.Enqueue(3);
            q.Enqueue(1);

            for (var i = 0; i < 5; i++)
            {
                var dequeued = q.Dequeue();
                Assert.Equal(dequeued, i);
            }
        }

        [Fact]
        public void PriorityQueueAddElementsPeekReturnsHighestPriorityMessage()
        {
            var q = new PriorityQueue<int>();

            q.Enqueue(0);
            q.Enqueue(4);
            q.Enqueue(2);
            q.Enqueue(3);
            q.Enqueue(1);

            var dequeued = q.Peek();

            Assert.Equal(dequeued, 0);
        }

        [Fact]
        public void PriorityQueueAddSameElements()
        {
            const int testSize = 100;

            var q = new PriorityQueue<int>();
            for (var i = 0; i < testSize; i++)
            {
                var count = q.Count;
                q.Enqueue(AnyInt);
                Assert.Equal(count + 1, q.Count);
            }

            Assert.Equal(testSize, q.Count);

            for (var i = 0; i < testSize; i++)
            {
                var count = q.Count;
                Assert.Equal(AnyInt, q.Dequeue());
                Assert.Equal(count - 1, q.Count);
            }

            Assert.Equal(0, q.Count);
        }

        [Fact]
        public void PriorityQueueRandomAddElementsDequeueReturnsInPriorityOrder()
        {
            const int testSize = 500;
            const int testTimes = 5;
            var r = new Random();

            for (var t = 0; t < testTimes; t++)
            {
                var q = new PriorityQueue<int>();
                var testArr = new int[testSize];
                for (var i = 0; i < testSize; i++)
                {
                    var num = r.Next();
                    testArr[i] = num;
                    q.Add(num);
                }
                
                Array.Sort(testArr);
                for (var i = 0; i < testSize; i++)
                {
                    Assert.Equal(testSize - i, q.Count);
                    Assert.Equal(testArr[i], q.Dequeue());
                }

                Assert.Equal(q.Count, 0);
            }
        }

        [Fact]
        public void PriorityQueueRandomAddDequeueInterweave()
        {
            const int addSize = 500;
            const int testTimes = 5;
            var r = new Random();

            for (var t = 0; t < testTimes; t++)
            {
                var q = new PriorityQueue<int>();
                var testList = new List<int>();
                for (var i = 0; i < addSize; i++)
                {
                    var num = r.Next();
                    testList.Add(num);
                    q.Add(num);

                    var shouldRemove = r.Next(0, 10) >= 5;
                    if (shouldRemove)
                    {
                        var removed = q.Dequeue();
                        Assert.Equal(testList.Min(), removed);
                        testList.Remove(removed);
                    }
                }

                testList.Sort();

                var testListSize = testList.Count;
                Assert.Equal(testListSize, q.Count);

                for (var i = 0; i < testListSize; i++)
                {
                    Assert.Equal(testListSize - i, q.Count);
                    Assert.Equal(testList[i], q.Dequeue());
                }

                Assert.Equal(q.Count, 0);
            }
        }
    }
}
