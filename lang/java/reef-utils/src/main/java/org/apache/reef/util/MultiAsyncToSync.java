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

import org.apache.reef.util.exception.InvalidBlockedCallerIdentifierException;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Assists a class in presenting a synchronous interface that is implemented
 * via asynchronous interfaces and events. When a method call is received
 * by the interface, parameter values are captured and asynchronous processing
 * started, the caller is put to sleep by calling block() with the internal
 * interface caller identifier. After all of the asynchronous processing
 * is complete the caller is released with a call to release().
 */
public final class MultiAsyncToSync {
  private static final Logger LOG = Logger.getLogger(MultiAsyncToSync.class.getName());

  private final ArrayDeque<ComplexCondition> freeQueue = new ArrayDeque<>();
  private final HashMap<Long, ComplexCondition> sleeperMap = new HashMap<>();
  private final long timeoutPeriod;
  private final TimeUnit timeoutUnits;

  /**
   * Initialize a multiple asynchronous to synchronous object with a specified timeout value.
   * @param timeoutPeriod The length of time in units given by the the timeoutUnits
   *                      parameter before the condition automatically times out.
   * @param timeoutUnits The unit of time for the timeoutPeriod parameter.
   */
  public MultiAsyncToSync(final long timeoutPeriod, final TimeUnit timeoutUnits) {
    this.timeoutPeriod = timeoutPeriod;
    this.timeoutUnits = timeoutUnits;
  }

  /**
   * Put the caller to sleep on a specific release identifier.
   * @param identifier The identifier required to awake the caller via the release() method.
   * @return A boolean value that indicates whether or not a timeout occurred.
   * @throws InterruptedException The thread was interrupted while waiting on a condition.
   */
  public boolean block(final long identifier) throws InterruptedException, InvalidBlockedCallerIdentifierException {

    // Reuse or allocate a condition for the call.
    ComplexCondition call = allocate();

    final boolean timeoutOccurred;
    call.preop();
    try {
      addSleeper(identifier, call);

      // Put the call to sleep until the ack comes back.
      LOG.log(Level.FINER, "Putting caller to sleep on identifier [{0}]", identifier);
      timeoutOccurred = call.waitOp();

      if (timeoutOccurred) {
        LOG.log(Level.FINER, "Caller sleeping on identifier [{0}] timed out", identifier);
        removeSleeper(identifier);
        recycle(call);
      }
    } finally {
      call.postop();
    }
    return timeoutOccurred;
  }

  /**
   * Wake the caller sleeping on the specific identifier.
   * @param identifier The message identifier of the caller who should be released.
   */
  public void release(final long identifier) throws InterruptedException, InvalidBlockedCallerIdentifierException {
    ComplexCondition call = getSleeper(identifier);
    call.preop();
    try {
      removeSleeper(identifier);
      LOG.log(Level.FINER, "Waking caller sleeping on identifier [{0}]", identifier);
      call.signalOp();
    } finally {
      call.postop();
      recycle(call);
    }
  }

  private ComplexCondition allocate() {
    final ComplexCondition call;
    synchronized (freeQueue) {
      if (!freeQueue.isEmpty()) {
        call = freeQueue.getFirst();
      } else {
        call = new ComplexCondition(timeoutPeriod, timeoutUnits);
      }
    }
    return call;
  }

  private void recycle(final ComplexCondition call) {
    if (null != call) {
      synchronized (freeQueue) {
        freeQueue.addLast(call);
      }
    }
  }

  private void addSleeper(final long identifier, final ComplexCondition call) {
    synchronized (sleeperMap) {
      if (sleeperMap.put(identifier, call) != null) {
        throw new RuntimeException(String.format("Duplicate identifier [%d] in sleeper map", identifier));
      }
    }
  }

  private ComplexCondition getSleeper(final long identifier) throws InvalidBlockedCallerIdentifierException {
    final ComplexCondition call;
    synchronized (sleeperMap) {
      call = sleeperMap.get(identifier);
      if (null == call) {
        throw new InvalidBlockedCallerIdentifierException(identifier);
      }
    }
    return call;
  }

  private void removeSleeper(final long identifier) throws InvalidBlockedCallerIdentifierException {
    final ComplexCondition call;
    synchronized (sleeperMap) {
      call = sleeperMap.remove(identifier);
      if (null == call) {
        throw new InvalidBlockedCallerIdentifierException(identifier);
      }
    }
  }
}
