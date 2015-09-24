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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.management.ThreadInfo;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test DeadlockInfo by creating a deadlock.
 */

public final class DeadlockInfoWithDeadlockPresentTest {
  private static final Logger LOG = Logger.getLogger(DeadlockInfoWithDeadlockPresentTest.class.getName());

  private static final long TIMEOUT_MILLIS = 100;

  private static final Lock LOCK_1 = new ReentrantLock();
  private static final Lock LOCK_2 = new ReentrantLock();

  private static Thread thread1;
  private static Thread thread2;

  /**
   * Create a deadlock consisting of two threads.
   *
   * setUpClass sleeps for TIMEOUT_MILLIS to allow the threads to progress into deadlock.
   */
  @BeforeClass
  public static void setUpClass() {
    createDeadlock();
    threadSleep(TIMEOUT_MILLIS);
  }

  /**
   * Remove  the deadlock by interrupting the first thread.
   *
   * This ensures that future DeadlockInfo test does not
   * detect the unnecessary deadlock.
   */
  @AfterClass
  public static void tearDownClass() {
    thread1.interrupt();
    threadSleep(TIMEOUT_MILLIS);
  }

  /**
   * The first thread holds the LOCK_1, and waits interruptibly on the LOCK_2;
   * the other one holds the LOCK_2 and waits on the LOCK_1.
   *
   * The barrier in between lock acquisition ensures that the deadlock occurs.
   *
   * Since there is no way to kill a thread, to resolve the deadlock we instead
   * interrupt the first thread and simply allow it to finish. This releases LOCK_1.
   *
   * The second thread should then terminate normally.
   */
  private static void createDeadlock() {

    final CyclicBarrier barrier = new CyclicBarrier(2);

    thread1 = new Thread() {
          @Override
          public void run() {

            try {
              LOCK_1.lock();
              barrierAwait(barrier);
              LOCK_2.lockInterruptibly();
            } catch (InterruptedException e) {
              LOG.info(Thread.currentThread().getName() + " is interrupted."
                      + " This interrupt is expected because it resolves the deadlock.");
            }

          }
      };

    thread2 = new Thread() {
          @Override
          public void run() {
            LOCK_2.lock();
            barrierAwait(barrier);
            LOCK_1.lock();
          }
      };

    thread1.start();
    thread2.start();

  }

  /**
   * Test that DeadlockInfo returns the expected values given the deadlock.
   */
  @Test
  public void testDeadlockInfo() {

    final DeadlockInfo deadlockInfo = new DeadlockInfo();

    final ThreadInfo[] threadInfos = deadlockInfo.getDeadlockedThreads();
    assertEquals("There must be two deadlocked threads", 2, threadInfos.length);

    for (final ThreadInfo threadInfo : deadlockInfo.getDeadlockedThreads()) {
      final String waitingLockString = deadlockInfo.getWaitingLockString(threadInfo);
      assertNotNull("Each thread should wait on a lock and"
              + " hence have the non-null waitingLockString", waitingLockString);
      assertTrue("Each Thread should wait on the ReentrantLock", waitingLockString.contains("ReentrantLock"));
    }

  }

  @Test
  public void testLogDeadlockInfo() {
    LOG.log(Level.INFO, ThreadLogger.getFormattedDeadlockInfo("Deadlock test, this deadlock is expected"));
  }

  /**
   * Once the barrier is met, the threads proceed to deadlock.
   */
  private static void barrierAwait(final CyclicBarrier barrier) {
    try {
      barrier.await(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      e.printStackTrace();
      fail("Unexpected exception");
    } catch (final BrokenBarrierException e) {
      e.printStackTrace();
      fail("Unexpected exception");
    } catch (final TimeoutException e) {
      e.printStackTrace();
      fail("Unexpected exception");
    }
  }

  private static void threadSleep(final long millis) {
    try {
      Thread.sleep(millis);
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
  }

}
