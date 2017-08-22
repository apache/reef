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

import java.util.concurrent.FutureTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
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
   * @param asyncProcessor A {@code FutureTask} object which returns {@code TAsync} that initiates the asynchronous
   *                       processing associated with the call. This will occur inside the condition lock
   *                       to prevent the processing from generating the signal before the calling thread blocks.
   *                       Error conditions should be handled by throwing an exception which the caller
   *                       will catch.
   * @param <TAsync> The return type of the {@code asyncProcessor};
   * @return A boolean value that indicates whether or not a timeout or error occurred.
   * @throws InterruptedException The thread was interrupted while waiting on a condition.
   * @throws InvalidIdentifierException The identifier parameter is invalid.
   * @throws Exception The future task object referenced by the {@code asyncProcessor}
   *                   parameter threw an exception.
   */
  public <TAsync> boolean block(final long identifier, final FutureTask<TAsync> asyncProcessor) throws Exception {
    final boolean timeoutOccurred;
    final ComplexCondition call = allocate();
    call.lock();
    try {
      // Add the call identifier to the sleeper map so release() can identify this instantiation.
      addSleeper(identifier, call);
      // Invoke the caller's asynchronous processing while holding the lock
      // so a wakeup cannot occur before the caller sleeps.
      asyncProcessor.run();
      // Put the caller to sleep until the ack comes back. Note: we atomically
      // give up the look as the caller sleeps and atomically reacquire the
      // the lock as we wake up.
      LOG.log(Level.FINER, "Putting caller to sleep on identifier [{0}]", identifier);
      timeoutOccurred = call.await();
      if (timeoutOccurred) {
        LOG.log(Level.SEVERE, "Call timed out on identifier [{0}]", identifier);
      }
    } finally {
      // Whether or not the call completed successfully, always remove
      // the call from the sleeper map, release the lock and cleanup.
      removeSleeper(identifier);
      recycle(call);
      call.unlock();
    }
    return timeoutOccurred;
  }

  /**
   * Wake the caller sleeping on the specific identifier.
   * @param identifier The message identifier of the caller who should be released.
   */
  public void release(final long identifier) throws InterruptedException, InvalidIdentifierException {
    final ComplexCondition call = getSleeper(identifier);
    call.lock();
    try {
      LOG.log(Level.FINER, "Waking caller sleeping on identifier [{0}]", identifier);
      call.signal();
    } finally {
      call.unlock();
    }
  }

  /**
   * Allocate a condition variable. May reuse existing ones.
   * @return A complex condition object.
   */
  private ComplexCondition allocate() {
    final ComplexCondition call = freeQueue.poll();
    return call != null ? call : new ComplexCondition(timeoutPeriod, timeoutUnits);
  }

  /**
   * Return a complex condition object to the free queueu.
   * @param call The complex condition to be recycled.
   */
  private void recycle(final ComplexCondition call) {
    freeQueue.add(call);
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
