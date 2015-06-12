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
package org.apache.reef.io.network.group.impl.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class CountingSemaphore {

  private static final Logger LOG = Logger.getLogger(CountingSemaphore.class.getName());

  private final AtomicInteger counter;

  private final String name;

  private final Object lock;

  private final int initCount;

  public CountingSemaphore(final int initCount, final String name, final Object lock) {
    super();
    this.initCount = initCount;
    this.name = name;
    this.lock = lock;
    this.counter = new AtomicInteger(initCount);
    LOG.finest("Counter initialized to " + initCount);
  }

  public int getInitialCount() {
    return initCount;
  }

  public int increment() {
    synchronized (lock) {
      final int retVal = counter.incrementAndGet();
      LOG.finest(name + "Incremented counter to " + retVal);
      logStatus();
      return retVal;
    }
  }

  private void logStatus() {
    final int yetToRun = counter.get();
    final int curRunning = initCount - yetToRun;
    LOG.fine(name + curRunning + " workers are running & " + yetToRun + " workers are yet to run");
  }

  public int decrement() {
    synchronized (lock) {
      final int retVal = counter.decrementAndGet();
      LOG.finest(name + "Decremented counter to " + retVal);
      if (retVal < 0) {
        LOG.warning("Counter negative. More workers exist than you expected");
      }
      if (retVal <= 0) {
        LOG.finest(name + "All workers are done with their task. Notifying waiting threads");
        lock.notifyAll();
      } else {
        LOG.finest(name + "Some workers are not done yet");
      }
      logStatus();
      return retVal;
    }
  }

  public int get() {
    synchronized (lock) {
      return counter.get();
    }
  }

  public void await() {
    synchronized (lock) {
      LOG.finest(name + "Waiting for workers to be done");
      while (counter.get() > 0) {
        try {
          lock.wait();
          LOG.finest(name + "Notified with counter=" + counter.get());
        } catch (final InterruptedException e) {
          throw new RuntimeException("InterruptedException while waiting for counting semaphore counter", e);
        }
      }
      LOG.finest(name + "Returning from wait");
    }
  }

}
