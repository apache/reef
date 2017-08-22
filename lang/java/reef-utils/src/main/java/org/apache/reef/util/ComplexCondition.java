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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages Java lock and condition objects to create a simplified
 * condition variable interface.
 */
public final class ComplexCondition {
  private final Lock lockVar = new ReentrantLock();
  private final Condition conditionVar = lockVar.newCondition();
  private final long timeoutPeriod;
  private final TimeUnit timeoutUnits;
  private static final long DEFAULT_TIMEOUT = 10;

  /**
   * Default constructor which initializes timeout period to 10 seconds.
   */
  public ComplexCondition() {
    this(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
  }

  /**
   * Wrap a lock and associated condition together into a single atomic condition
   * variable that can be used synchronize waiters and signalers.
   * Typical usage:
   * {@code
   *   cv.lock();
   *   try {
   *     // access shared objects.
   *     cv.await(); // or cv.signal()
   *     // access shared objects.
   *   } finally {
   *     cv.unlock();
   *   }
   * }
   * @param timeoutPeriod The length of time in units given by the the timeoutUnits
   *                      parameter before the condition automatically times out.
   * @param timeoutUnits The unit of time for the timeoutPeriod parameter.
   */
  public ComplexCondition(final long timeoutPeriod, final TimeUnit timeoutUnits) {
    this.timeoutPeriod = timeoutPeriod;
    this.timeoutUnits = timeoutUnits;
  }

  /**
   * Declare a threads intention to either wait or signal the condition. Any work
   * with objects that are shared between the waiter and signaler should only be
   * accessed after calling {@code preop()} and before calling {@code await()} or
   * {@code signal()}.
   */
  public void lock() {
    lockVar.lock();
  }

  /**
   * Declare a threads intention release the condition after a call to wait or signal.
   * Any work with objects that are shared between the waiter and signaler should only
   * be access after {@code await()} or {@code signal()} and before
   * calling {@code unlock()}.
   */
  public void unlock() {
    lockVar.unlock();
  }

  /**
   * Wait for a signal on the condition. Must call {@code lock()} first
   * and {@code unlock()} afterwards.
   * @return A boolean value that indicates whether or not a timeout occurred.
   * @throws InterruptedException The calling thread was interrupted by another thread.
   */
  public boolean await() throws InterruptedException {
    return !conditionVar.await(timeoutPeriod, timeoutUnits);
  }

  /**
   * Signal the sleeper on the condition. Must have called {@code lock()} first
   * and {@code unlock()} afterwards.
   */
  public void signal() {
    conditionVar.signal();
  }
}
