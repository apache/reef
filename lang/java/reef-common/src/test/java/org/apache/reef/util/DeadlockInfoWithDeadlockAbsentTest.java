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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Test DeadlockInfo by requesting information about
 * non-deadlocked threads.
 *
 * Reproduces REEF-588.
 */
public class DeadlockInfoWithDeadlockAbsentTest {

  private static final Logger LOG = Logger.getLogger(DeadlockInfoWithDeadlockAbsentTest.class.getName());

  private static final long TIMEOUT_MILLIS = 100;

  private static final CountDownLatch FIRST_LATCH = new CountDownLatch(1);
  private static final CountDownLatch SECOND_LATCH = new CountDownLatch(1);

  /**
    * Create a situation where a deadlock is possible but not present.
    *
    * Sleep for TIMEOUT_MILLIS to allow this situation to set up.
   *
  */
  @BeforeClass
  public static void setUp() {
    startNonDeadlockedThreads();
    threadSleep(TIMEOUT_MILLIS);
  }

  /**
   * Test the normal DeadlockInfo behaviour in the
   * absence of deadlocks.
   *
   * DeadlockInfo instantiation reproduces the REEF-588
   * because the NPE was thrown in the DeadlockInfo constructor.
   */
  @Test
  public void testDeadlockInfoWithDeadlockAbsent()
          throws NullPointerException {

    final DeadlockInfo deadlockInfo = new DeadlockInfo();
    LOG.log(Level.INFO, ThreadLogger.getFormattedDeadlockInfo(
              "DeadlockInfo test, none deadlocks expected. Deadlocks found: "));
    Assert.assertEquals("DeadlockInfo found deadlocks when none should exist.", 0,
              deadlockInfo.getDeadlockedThreads().length);

  }

  /**
   * Test logging in the absence of deadlocks.
   */
  @Test
  public void testLogDeadlockInfo() throws NullPointerException {
    LOG.log(Level.INFO, ThreadLogger.getFormattedDeadlockInfo(
            "DeadlockInfo test, none deadlocks expected. Deadlocks found: "));
  }

  /**
   * Create a situation where a deadlock is possible but not present.
   *
   * Assume there are two resources guarded by latches.
   * The deadlock is possible if multiple threads attempt to acquire both latches.
   *
   * Spawn two threads so that each thread acquires only one latch.
   * By design circular wait between the threads can't occur, so the deadlock can't exist.
   *
   * The threads wait on their latches until the DeadlockInfo tests finish.
   * Then the tearDown() wakes up the threads to avoid liveness issues.
   */
  private static void startNonDeadlockedThreads() {

    final Thread thread1 = new Thread(){
        @Override
        public void run(){
            awaitOnLatch(FIRST_LATCH);
        }
    };

    final Thread thread2 = new Thread(){
        @Override
        public void run(){
            awaitOnLatch(SECOND_LATCH);
        }
    };

    thread1.start();
    thread2.start();
  }

  /**
   * Allow both threads to finish.
   */
  @AfterClass
  public static void tearDown() {
    FIRST_LATCH.countDown();
    SECOND_LATCH.countDown();
  }

  private static void awaitOnLatch(final CountDownLatch latch) {
    try {
      latch.await();
    } catch (final InterruptedException e) {
      e.printStackTrace();
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
