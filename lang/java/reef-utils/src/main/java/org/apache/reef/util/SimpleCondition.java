/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.reef.util;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages Java lock and condition objects to create a simplified
 * condition variable interface.
 */
public final class SimpleCondition {
  private final Lock lockVar = new ReentrantLock();
  private final Condition conditionVar = lockVar.newCondition();
  private final long timeoutPeriod;
  private final TimeUnit timeoutUnits;
  private static final long DEFAULT_TIMEOUT = 10;

  /**
   * Default constructor which initializes timeout period to 10 seconds.
   */
  public SimpleCondition() {
    this(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
  }

  /**
   * Initialize condition variable with user specified timeout.
   * @param timeoutPeriod The length of time in units given by the the timeoutUnits
   *                      parameter before the condition automatically times out.
   * @param timeoutUnits The unit of time for the timeoutPeriod parameter.
   */
  public SimpleCondition(final long timeoutPeriod, final TimeUnit timeoutUnits) {
    this.timeoutPeriod = timeoutPeriod;
    this.timeoutUnits = timeoutUnits;
  }

  /**
   * Blocks the caller until {@code signal()} is called or a timeout occurs.
   * Logical structure:
   * {@code
   *   cv.lock();
   *   try {
   *     doTry();
   *     cv.await(); // or cv.signal()
   *   } finally {
   *     doFinally();
   *     cv.unlock();
   *   }
   * }
   * @param doTry A {@code Callable<TTry>} object that is called after the internal
   *              condition lock is taken but before waiting on the condition occurs.
   * @param doFinally A {@code Callable<TFinally>} object that is called after the wakeup
   *                  on the condition occurs but before giving up the condition lock occurs.
   * @return A boolean value that indicates whether or not a timeout occurred.
   * @throws InterruptedException Thread was interrupted by another thread while
   * waiting for the signal.
   * @param <TTry>
   * @param <TFinally>
   * @throws Exception
   */
  public <TTry, TFinally> boolean await(Callable<TTry> doTry, Callable<TFinally> doFinally) throws Exception {
    final boolean timeoutOccurred;
    lockVar.lock();
    try {
      if (null != doTry) {
        doTry.call();
      }
      timeoutOccurred = !conditionVar.await(timeoutPeriod, timeoutUnits);
    } finally {
      if (null != doFinally) {
        doFinally.call();
      }
      lockVar.unlock();
    }
    return timeoutOccurred;
  }

  /**
   * Wakes the thread sleeping in await().
   */
  public void signal() {
    lockVar.lock();
    try {
      conditionVar.signal();
    } finally {
      lockVar.unlock();
    }
  }
}
