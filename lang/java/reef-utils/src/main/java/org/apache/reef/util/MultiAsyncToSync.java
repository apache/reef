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

import org.apache.reef.util.exception.InvalidIdentifierException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Assists a class in presenting a synchronous interface that is implemented
 * via asynchronous interfaces and events. When a method call is received
 * by the interface, parameter values are captured and the initiation of
 * asynchronous processing is encapsulated in a callable object. When
 * {@code block()} is called with the internal interface identifier, the
 * lock is taken, the asynchronous processing is initiated, and the caller
 * is put to sleep. After all of the asynchronous processing is complete the
 * caller is released with a call to {@code release()}.
 */
public final class MultiAsyncToSync {
  private static final Logger LOG = Logger.getLogger(MultiAsyncToSync.class.getName());

  private final ConcurrentLinkedQueue<ComplexCondition> freeQueue = new ConcurrentLinkedQueue<>();
  private final ConcurrentHashMap<Long, ComplexCondition> sleeperMap = new ConcurrentHashMap<>();
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
   * @param identifier The identifier required to awake the caller via the {@code release()} method.
   * @param asyncProcessor A callable object that initiates the asynchronous processing associated
   *                       with the call. This will occur inside the condition lock to prevent
   *                       the processing from generating the signal before the calling thread blocks.
   * @return A boolean value that indicates whether or not a timeout occurred.
   * @throws InterruptedException The thread was interrupted while waiting on a condition.
   * @throws InvalidIdentifierException The identifier parameter is invalid.
   * @throws Exception The callable object referenced by the asyncProcessor parameter threw an exception.
   */
  public boolean block(final long identifier,
                       final Callable<Boolean> asyncProcessor) throws Exception {
    boolean timeoutOccurred = false;
    ComplexCondition call = allocate();
    call.takeLock();
    try {
      addSleeper(identifier, call);
      if (asyncProcessor.call()) {
        // Put the call to sleep until the ack comes back.
        LOG.log(Level.FINER, "Putting caller to sleep on identifier [{0}]", identifier);
        timeoutOccurred = call.waitForSignal();

        if (timeoutOccurred) {
          LOG.log(Level.FINER, "Caller sleeping on identifier [{0}] timed out", identifier);
          removeSleeper(identifier);
          recycle(call);
        }
      } else {
        removeSleeper(identifier);
      }
    } finally {
      call.releaseLock();
    }
    return timeoutOccurred;
  }

  /**
   * Wake the caller sleeping on the specific identifier.
   * @param identifier The message identifier of the caller who should be released.
   */
  public void release(final long identifier)
        throws InterruptedException, InvalidIdentifierException {
    ComplexCondition call = getSleeper(identifier);

    call.takeLock();
    try {
      removeSleeper(identifier);
      LOG.log(Level.FINER, "Waking caller sleeping on identifier [{0}]", identifier);
      call.signalCondition();
    } finally {
      call.releaseLock();
      recycle(call);
    }
  }

  /**
   * Allocate a condition variable. May reuse existing ones.
   * @return A complex condition object.
   */
  private ComplexCondition allocate() {
    ComplexCondition call = freeQueue.poll();
    if (null == call) {
      call = new ComplexCondition(timeoutPeriod, timeoutUnits);
    }
    return call;
  }

  /**
   * Return a complex condition object to the free queueu.
   * @param call The complex condition to be recycled.
   */
  private void recycle(final ComplexCondition call) {
    if (null != call) {
      freeQueue.add(call);
    }
  }

  /**
   * Atomically add a coll to the sleeper map.
   * @param identifier The unique call identifier.
   * @param call The call object to be added to the sleeper map.
   */
  private void addSleeper(final long identifier, final ComplexCondition call) {
    if (sleeperMap.put(identifier, call) != null) {
      throw new RuntimeException(String.format("Duplicate identifier [%d] in sleeper map", identifier));
    }
  }

  /**
   * Get a reference to a sleeper with a specific identifier without removing
   * it from the sleeper map.
   * @param identifier The unique identifier of the sleeper to be retrieved.
   * @return The complex condition object associated with the input identifier.
   * @throws InvalidIdentifierException The sleeper map does not contain a call
   * with the specified identifier.
   */
  private ComplexCondition getSleeper(final long identifier) throws InvalidIdentifierException {
    final ComplexCondition call = sleeperMap.get(identifier);
    if (null == call) {
      throw new InvalidIdentifierException(identifier);
    }
    return call;
  }

  /**
   * Remove the specified call from the sleeper map.
   * @param identifier The unique identifier of the call to be removed.
   * @throws InvalidIdentifierException The sleeper map does not contain a call
   * with the specified identifier.
   */
  private void removeSleeper(final long identifier) throws InvalidIdentifierException {
    final ComplexCondition call = sleeperMap.remove(identifier);
    if (null == call) {
      throw new InvalidIdentifierException(identifier);
    }
  }
}
