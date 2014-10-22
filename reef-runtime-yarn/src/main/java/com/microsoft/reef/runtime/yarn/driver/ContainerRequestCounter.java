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
package com.microsoft.reef.runtime.yarn.driver;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Used to keep track of resource requests.
 */
@Private
@DriverSide
final class ContainerRequestCounter {
  private static final Logger LOG = Logger.getLogger(ContainerRequestCounter.class.getName());

  private int counter = 0;

  @Inject
  ContainerRequestCounter() {
    LOG.log(Level.FINEST, "Instantiated 'ContainerRequestCounter'");
  }

  /**
   * Increment the counter by the given amount.
   *
   * @param number
   */
  synchronized void incrementBy(final int number) {
    this.counter += number;
  }

  /**
   * @return the current value of the counter.
   */
  synchronized int get() {
    return this.counter;
  }

  /**
   * Decrement the counter by 1.
   */
  synchronized void decrement() {
    if (this.counter <= 0) {
      LOG.log(Level.WARNING, "requestedContainerCount cannot go below 0");
      this.counter = 0;
    } else {
      this.counter -= 1;
    }
  }

}
