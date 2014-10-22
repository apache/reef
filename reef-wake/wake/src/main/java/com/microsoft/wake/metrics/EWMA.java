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
package com.microsoft.wake.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Exponentially weighted moving average
 */
public class EWMA {

  private final double alpha;
  private final double interval;

  private volatile boolean initialized = false;
  private volatile double rate = 0.0;

  private final AtomicLong uncounted = new AtomicLong();

  /**
   * Constructs an EWMA object
   * 
   * @param alpha
   * @param interval
   * @param intervalUnit
   */
  public EWMA(double alpha, long interval, TimeUnit intervalUnit) {
    this.interval = intervalUnit.toNanos(interval);
    this.alpha = alpha;
  }
  
  /**
   * Updates the EWMA with a new value 
   * 
   * @param n the new value
   */
  public void update(long n) {
    uncounted.addAndGet(n);
  }   
  
  /**
   * Updates the rate
   */
  public void tick() {
    final long count = uncounted.getAndSet(0);
    final double instantRate = count / interval;
    if (initialized) {
        rate += (alpha * (instantRate - rate));
    } else {
        rate = instantRate;
        initialized = true;
    }
  }
  
  /**
   * Gets the rate
   * 
   * @param rateUnit the unit of the rate
   * @return the rate
   */
  public double getRate(TimeUnit rateUnit) {
    return rate * (double) rateUnit.toNanos(1);
  }
}
