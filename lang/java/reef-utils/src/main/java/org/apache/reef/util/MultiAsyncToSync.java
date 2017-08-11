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

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Assists a class in presenting a synchronous interface that is implemented
 * via asynchronous interfaces and events. When a method call is received
 * by the interface, parameter values captured and asynchronous processing
 * started, the caller is put to sleep by calling block() with the internal
 * interface caller identifier. After all of the asynchronous processing
 * is complete the caller is released with a call to release().
 */
public final class MultiAsyncToSync {
  private static final Logger LOG = Logger.getLogger(MultiAsyncToSync.class.getName());

  private final ArrayDeque<ConditionVariable> freeQueue = new ArrayDeque<>();
  private final HashMap<Long, ConditionVariable> sleeperMap = new HashMap<>();
  private final long timeoutPeriod;
  private final TimeUnit timeoutUnits;

  /**
   * Initialize a multiple asynchronous to synchronous object with a specified timeout value.
   * @param timeoutPeriod The length of time in units geven by the the timeoutUnits
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
   * @return
   */
  public boolean block(final long identifier) {
    ConditionVariable call;
    synchronized (this) {
      // Get an condition variable to block the calling thread.
      if (sleeperMap.containsKey(identifier)) {
        // This should never happen as message identifiers are unique.
        LOG.log(Level.SEVERE, "Duplicate identifier in RPC map");
      }
      if (freeQueue.isEmpty()) {
        freeQueue.addLast(new ConditionVariable(timeoutPeriod, timeoutUnits));
      }
      call = freeQueue.getFirst();
      sleeperMap.put(identifier, call);
    }

    LOG.log(Level.INFO, "Putting caller to sleep on identifier [{0}]", identifier);
    // Put the call to sleep until the ack comes back.
    boolean timeoutOccurred = call.waitForSignal();
    if (timeoutOccurred) {
      synchronized (this) {
        call = sleeperMap.remove(identifier);
        freeQueue.addLast(call);
      }
      LOG.log(Level.INFO, "Caller sleeping on identifier [{0}] timed out", identifier);
    }
    return timeoutOccurred;
  }

  /**
   * Wake the caller sleeping on the specific identifier.
   * @param identifier The message identifier of the caller who should be released.
   */
  public void release(final long identifier) {
    ConditionVariable call;
    synchronized (this) {
      // Get the associated call object.
      call = sleeperMap.remove(identifier);
      if (call == null) {
        LOG.log(Level.SEVERE, "Unknown message identifier [{0}]", identifier);
      } else {
        // Signal the sleeper and recycle the call object.
        LOG.log(Level.INFO, "Waking caller sleeping on identifier [{0}]", identifier);
        call.signalWaitComplete();
        freeQueue.addLast(call);
      }
    }
  }
}
