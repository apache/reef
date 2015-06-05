/*
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
package org.apache.reef.util;

import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.management.ThreadInfo;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Test DeadlockInfo by creating a deadlock
 */
public final class DeadlockInfoTest {
  private static final Logger LOG = Logger.getLogger(DeadlockInfoTest.class.getName());

  private static final long timeoutMillis = 50;

  /**
   * Create a deadlock consisting of two threads.
   * The threads wait on a barrier, and once the barrier is met they proceed to deadlock.
   * setUpClass sleeps for timeoutMillis to allow the threads time to progress past the barrier into deadlock.
   *
   * One thread holds an Object and Long lock, and is waiting on an Integer lock.
   * The other thread holds the Integer lock and is waiting on the Long lock.
   */
  @BeforeClass
  public static void setUpClass() {
    createDeadlock();
    threadSleep(timeoutMillis);
  }

  /**
   * Create a deadlock consisting of two threads,
   * then test that DeadlockInfo returns the expected values given the deadlock.
   *
   * One thread holds an Object and Long lock, and is waiting on an Integer lock.
   * The other thread holds the Integer lock and is waiting on the Long lock.
   */
  @Test
  public void testDeadlockInfo() {
    final DeadlockInfo deadlockInfo = new DeadlockInfo();

    final ThreadInfo[] threadInfos = deadlockInfo.getDeadlockedThreads();
    assertEquals(2, threadInfos.length);

    for (final ThreadInfo threadInfo : deadlockInfo.getDeadlockedThreads()) {
      final String waitingLockString = deadlockInfo.getWaitingLockString(threadInfo);
      assertNotNull("Each thread is expected to have a waiting lock", waitingLockString);
      if (waitingLockString.contains("Integer")) {
        assertNumberOfLocksHeld(2, deadlockInfo, threadInfo);
      } else if (waitingLockString.contains("Long")) {
        assertNumberOfLocksHeld(1, deadlockInfo, threadInfo);
      } else {
        fail("Unexpected waitingLockString of "+waitingLockString);
      }
    }
  }

  @Test
  public void testLogDeadlockInfo() {
    LOG.log(Level.INFO, ThreadLogger.getFormattedDeadlockInfo("Deadlock test, this deadlock is expected"));
  }

  private static void assertNumberOfLocksHeld(
      final int expected, final DeadlockInfo deadlockInfo, final ThreadInfo threadInfo) {
    int sum = 0;
    for (final StackTraceElement stackTraceElement : threadInfo.getStackTrace()) {
      sum += deadlockInfo.getMonitorLockedElements(threadInfo, stackTraceElement).size();
    }
    assertEquals(expected, sum);
  }

  private static void createDeadlock() {
    final CyclicBarrier barrier = new CyclicBarrier(2);

    final Integer lock1 = new Integer(0);
    final Long lock2 = new Long(0);

    final Thread thread1 = new Thread() {
      @Override
      public void run() {
        synchronized (lock1) {
          barrierAwait(barrier);
          lockLeaf(lock2);
        }
      }
    };

    final Thread thread2 = new Thread() {
      @Override
      public void run() {
        synchronized (new Object()) {
          synchronized (lock2) {
            barrierAwait(barrier);
            lockLeaf(lock1);
          }
        }
      }
    };

    thread1.start();
    thread2.start();
  }

  private static void barrierAwait(final CyclicBarrier barrier) {
    try {
      barrier.await(timeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();
      fail("Unexpected exception");
    } catch (BrokenBarrierException e) {
      e.printStackTrace();
      fail("Unexpected exception");
    } catch (TimeoutException e) {
      e.printStackTrace();
      fail("Unexpected exception");
    }
  }

  private static void lockLeaf(final Object lock) {
    synchronized (lock) {
      fail("The unit test failed to create a deadlock");
    }
  }

  private static void threadSleep(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
