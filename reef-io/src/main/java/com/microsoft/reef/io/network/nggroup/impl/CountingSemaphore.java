/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.nggroup.impl;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 *
 */
public class CountingSemaphore {

  private static final Logger LOG = Logger.getLogger(CountingSemaphore.class.getName());


  private final AtomicInteger counter;


  private final String name;


  private final Object lock;

  public CountingSemaphore(final int initCount, final String name, final Object lock) {
    super();
    this.name = name;
    this.lock = lock;
    this.counter = new AtomicInteger(initCount);
    LOG.info("Counter initialized to " + initCount);
  }

  public int increment() {
    synchronized (lock) {
      final int retVal = counter.incrementAndGet();
      LOG.info(name + "Incremented counter to " + retVal);
      return retVal;
    }
  }

  public int decrement() {
    synchronized (lock) {
      final int retVal = counter.decrementAndGet();
      LOG.info(name + "Decremented counter to " + retVal);
      if (retVal < 0) {
        LOG.warning("Counter negative. More workers exist than you expected");
      }
      if (retVal <= 0) {
        LOG.info(name
            + "All workers are done with their task. Notifying waiting threads");
        lock.notifyAll();
      } else {
        LOG.info(name + "Some workers are not done yet");
      }
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
      LOG.info(name + "Waiting for workers to be done");
      while (counter.get() > 0) {
        try {
          lock.wait();
          LOG.info(name + "Notified with counter=" + counter.get());
        } catch (final InterruptedException e) {
          throw new RuntimeException(
              "InterruptedException while waiting for counting semaphore counter",
              e);
        }
      }
      LOG.info(name + "Returning from wait");
    }
  }

}
