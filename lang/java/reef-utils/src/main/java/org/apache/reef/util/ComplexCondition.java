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
  private final Lock lock = new ReentrantLock();
  private final Condition condition = lock.newCondition();
  private final long timeoutPeriod;
  private final TimeUnit timeoutUnits;
  private static long defaultTimeout = 10;

  /**
   * Default constructor which initializes timeout period to 10 seconds.
   */
  public ComplexCondition() {
    this(defaultTimeout, TimeUnit.SECONDS);
  }

  /**
   * Initialize condition variable with user specified timeout.
   * @param timeoutPeriod The length of time in units given by the the timeoutUnits
   *                      parameter before the condition automatically times out.
   * @param timeoutUnits The unit of time for the timeoutPeriod parameter.
   */
  public ComplexCondition(final long timeoutPeriod, final TimeUnit timeoutUnits) {
    this.timeoutPeriod = timeoutPeriod;
    this.timeoutUnits = timeoutUnits;
  }

  public void preop() {
    lock.lock();
  }

  public void postop() {
    lock.unlock()
  }

  public boolean waitOp() throws InterruptedException {
    return !condition.await(timeoutPeriod, timeoutUnits);
  }

  public void signaOp() {
    condition.signal();
  }
}
