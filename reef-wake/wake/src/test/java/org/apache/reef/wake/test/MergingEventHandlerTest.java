/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.reef.wake.test;

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.MergingEventHandler;
import org.apache.reef.wake.impl.MergingEventHandler.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MergingEventHandlerTest {

  @Test
  public void testSingleInvocationSingleThread() {

    final int testLeft = 13;
    final int testRight = 23;
    final int expected = testLeft + 31 * testRight;

    final AtomicInteger i = new AtomicInteger(0);

    final MergingEventHandler<Integer, Integer> dut =
        new MergingEventHandler<>(new EventHandler<Pair<Integer, Integer>>() {
          @Override
          public void onNext(final Pair<Integer, Integer> value) {
            i.addAndGet(value.first + 31 * value.second);
          }
        });

    dut.left.onNext(testLeft);
    dut.right.onNext(testRight);

    Assert.assertEquals(expected, i.get());
  }

  @Test
  public void testSingleInvocationSingleThreadReversed() {

    final int testLeft = 11;
    final int testRight = 47;
    final int expected = testLeft + 17 * testRight;

    final AtomicInteger i = new AtomicInteger(0);

    final MergingEventHandler<Integer, Integer> dut =
        new MergingEventHandler<>(new EventHandler<Pair<Integer, Integer>>() {
          @Override
          public void onNext(final Pair<Integer, Integer> value) {
            i.addAndGet(value.first + 17 * value.second);
          }
        });

    dut.right.onNext(testRight);
    dut.left.onNext(testLeft);

    Assert.assertEquals(expected, i.get());
  }

  @Test
  public void testMultipleInvocationSingleThread() {

    final int testLeft1 = 13;
    final int testRight1 = 23;
    final int testLeft2 = 14;
    final int testRight2 = 1001;
    final int expected1 = testLeft1 + 31 * testRight1;
    final int expected2 = testLeft2 + 31 * testRight2;

    final AtomicInteger i = new AtomicInteger(0);

    final MergingEventHandler<Integer, Integer> dut =
        new MergingEventHandler<>(new EventHandler<Pair<Integer, Integer>>() {
          @Override
          public void onNext(final Pair<Integer, Integer> value) {
            i.addAndGet(value.first + 31 * value.second);
          }
        });

    dut.left.onNext(testLeft1);
    dut.right.onNext(testRight1);

    dut.left.onNext(testLeft2);
    dut.right.onNext(testRight2);

    Assert.assertEquals(expected1 + expected2, i.get());
  }

  @Test
  public void testMultipleInvocationMultipleThread() {

    final int testLeft1 = 13;
    final int testRight1 = 23;
    final int testLeft2 = 14;
    final int testRight2 = 1001;
    final int expected1 = testLeft1 + 31 * testRight1;
    final int expected2 = testLeft2 + 31 * testRight2;

    final AtomicInteger i = new AtomicInteger(0);

    final MergingEventHandler<Integer, Integer> dut =
        new MergingEventHandler<>(new EventHandler<Pair<Integer, Integer>>() {
          @Override
          public void onNext(final Pair<Integer, Integer> value) {
            i.addAndGet(value.first + 31 * value.second);
          }
        });

    // relies on Executor using both threads
    final ExecutorService pool = Executors.newFixedThreadPool(2);

    pool.submit(new Runnable() {
      @Override
      public void run() {
        dut.left.onNext(testLeft1);
        dut.right.onNext(testRight2);
      }
    });

    pool.submit(new Runnable() {
      @Override
      public void run() {
        dut.right.onNext(testRight1);
        dut.left.onNext(testLeft2);
      }
    });

    pool.shutdown();

    try {
      pool.awaitTermination(20, TimeUnit.SECONDS);
    } catch (final InterruptedException e1) {
      Assert.fail("Timeout waiting for events to fire, perhaps due to deadlock");
    }

    Assert.assertEquals(expected1 + expected2, i.get());
  }

  @Test
  public void testManyInvocations() {

    final int expectedEvents = 200;
    final int numLeftTasks = 2;
    final int numRightTasks = 4;

    final int eventsPerLeft = expectedEvents / numLeftTasks;
    Assert.assertEquals("Test parameters must divide",
        expectedEvents, numLeftTasks * eventsPerLeft);

    final int eventsPerRight = expectedEvents / numRightTasks;
    Assert.assertEquals("Test parameters must divide",
        expectedEvents, numRightTasks * eventsPerRight);

    final AtomicInteger i = new AtomicInteger(0);

    final MergingEventHandler<Integer, Integer> dut =
        new MergingEventHandler<>(new EventHandler<Pair<Integer, Integer>>() {
          @Override
          public void onNext(final Pair<Integer, Integer> value) {
            i.incrementAndGet();
          }
        });

    // relies on Executor making all tasks concurrent
    final ExecutorService pool = Executors.newCachedThreadPool();

    for (int l = 0; l < numLeftTasks; ++l) {
      pool.submit(new Runnable() {
        @Override
        public void run() {
          for (int kk = 0; kk < eventsPerLeft; ++kk) {
            dut.left.onNext(kk);
          }
        }
      });
    }

    for (int r = 0; r < numRightTasks; ++r) {
      pool.submit(new Runnable() {
        @Override
        public void run() {
          for (int kk = 0; kk < eventsPerRight; ++kk) {
            dut.right.onNext(kk);
          }
        }
      });
    }

    pool.shutdown();

    try {
      pool.awaitTermination(30, TimeUnit.SECONDS);
    } catch (final InterruptedException e1) {
      Assert.fail("Timeout waiting for events to fire, perhaps due to deadlock");
    }

    Assert.assertEquals(expectedEvents, i.get());
  }

  @Test
  public void testDifferentTypes() {
    final AtomicInteger i = new AtomicInteger(0);

    final MergingEventHandler<Boolean, Double> dut =
        new MergingEventHandler<>(new EventHandler<Pair<Boolean, Double>>() {
          @Override
          public void onNext(final Pair<Boolean, Double> value) {
            i.incrementAndGet();
          }
        });

    dut.left.onNext(true);
    dut.right.onNext(104.0);

    Assert.assertEquals(1, i.get());
  }
}
