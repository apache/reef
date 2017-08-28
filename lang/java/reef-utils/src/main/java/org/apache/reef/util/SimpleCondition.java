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

import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages Java lock and condition objects to create a simplified
 * condition variable interface.
 */
public final class SimpleCondition {
  private static final Logger LOG = Logger.getLogger(SimpleCondition.class.getName());
  private final ReentrantLock lockVar = new ReentrantLock();
  private final Condition conditionVar = lockVar.newCondition();
  private final long timeoutPeriod;
  private final TimeUnit timeoutUnits;
  private volatile boolean isSignal = false;

  /**
   * Default constructor which initializes timeout period to 10 seconds.
   */
  public SimpleCondition() {
    this(Long.MAX_VALUE, TimeUnit.DAYS);
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
   *     doTry.run();
   *     cv.await(); // or cv.signal()
   *   } finally {
   *     doFinally.run();
   *     cv.unlock();
   *   }
   * }
   * @param doTry A {@code FutureTask<TTry>} object that is run after the internal
   *              condition lock is taken but before waiting on the condition occurs.
   * @param doFinally A {@code FutureTask<TFinally>} object that is run after the wakeup
   *                  on the condition occurs but before giving up the condition lock
   *                  is released.
   * @param <TTry> The return type of the {@code doTry} future task.
   * @param <TFinally> The return type of the {@code doFinally} future task.
   * @return A boolean value that indicates whether or not a signal was received. False
   * indicates that a timeout occurred before a signal was received.
   * @throws InterruptedException Thread was interrupted by another thread while
   * waiting for the signal.
   * @throws Exception The callers (@code doTry} or {@code doFinally} future task
   *                   threw an exception.
   */
  public <TTry, TFinally> boolean await(final FutureTask<TTry> doTry,
                                        final FutureTask<TFinally> doFinally) throws Exception {
    boolean noTimeout = true;
    if (lockVar.isHeldByCurrentThread()) {
      throw new RuntimeException("signal() must not be called on same thread as await()");
    }
    try {
      lockVar.lock();
      if (null != doTry) {
        // Invoke the caller's asynchronous processing while holding the lock
        // so a wakeup cannot occur before the caller sleeps.
        doTry.run();
      }
      // Put the caller asleep on the condition until a signal is received
      // or a timeout occurs. Ignore spurious wake ups.
      LOG.log(Level.FINER, "Putting caller to sleep...");
      while (!isSignal && noTimeout) {
        noTimeout = conditionVar.await(timeoutPeriod, timeoutUnits);
      }
      LOG.log(Level.FINER, "Caller waking up...");
    } finally {
      isSignal = false;
      if (null != doFinally) {
        try {
          // Whether or not a timeout occurred, call the user's cleanup code.
          doFinally.run();
        } finally {
          lockVar.unlock();
        }
      } else {
        lockVar.unlock();
      }
    }
    return noTimeout;
  }

  /**
   * Wakes the thread sleeping in (@code await()}.
   */
  public void signal() {
    if (lockVar.isHeldByCurrentThread()) {
      throw new RuntimeException("signal() must not be called on same thread as await()");
    }
    try {
      lockVar.lock();
      LOG.log(Level.INFO, "Signalling sleeper...");
      isSignal = true;
      conditionVar.signal();
    } finally {
      lockVar.unlock();
    }
  }
}
