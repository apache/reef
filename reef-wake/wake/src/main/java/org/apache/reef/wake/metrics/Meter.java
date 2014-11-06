/**
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
package org.apache.reef.wake.metrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Meter that monitors mean throughput and ewma (1m, 5m, 15m) throughput
 */
public class Meter {

  private static final long TICK_INTERVAL = TimeUnit.SECONDS.toNanos(5);

  private final AtomicLong count = new AtomicLong();
  private final long startTime;
  private final AtomicLong lastTick;

  private final EWMA m1Thp;
  private final EWMA m5Thp;
  private final EWMA m15Thp;

  private final String name;

  /**
   * Constructs a meter
   *
   * @param name the name of the meter
   */
  public Meter(String name) {
    this.name = name;
    this.m1Thp = new EWMA(EWMAParameters.M1_ALPHA, EWMAParameters.INTERVAL, TimeUnit.SECONDS);
    this.m5Thp = new EWMA(EWMAParameters.M5_ALPHA, EWMAParameters.INTERVAL, TimeUnit.SECONDS);
    this.m15Thp = new EWMA(EWMAParameters.M15_ALPHA, EWMAParameters.INTERVAL, TimeUnit.SECONDS);
    this.startTime = getTick();
    this.lastTick = new AtomicLong(startTime);
  }

  /**
   * Gets the name of the meter
   *
   * @return the meter name
   */
  public String getName() {
    return name;
  }

  /**
   * Marks the number of events
   *
   * @param n the number of events
   */
  public void mark(long n) {
    tickIfNecessary();
    count.addAndGet(n);
    m1Thp.update(n);
    m5Thp.update(n);
    m15Thp.update(n);
  }

  /**
   * Gets the count
   *
   * @return the count
   */
  public long getCount() {
    return count.get();
  }

  /**
   * Gets the mean throughput
   *
   * @return the mean throughput
   */
  public double getMeanThp() {
    if (getCount() == 0) {
      return 0.0;
    } else {
      final double elapsed = (getTick() - startTime);
      return getCount() / elapsed * TimeUnit.SECONDS.toNanos(1);
    }
  }

  /**
   * Gets the 1-minute EWMA throughput
   *
   * @return the 1-minute EWMA throughput
   */
  public double get1mEWMAThp() {
    tickIfNecessary();
    return m1Thp.getRate(TimeUnit.SECONDS);
  }

  /**
   * Gets the 5-minute EWMA throughput
   *
   * @return the 5-minute EWMA throughput
   */
  public double get5mEWMAThp() {
    tickIfNecessary();
    return m5Thp.getRate(TimeUnit.SECONDS);
  }

  /**
   * Gets the 15-minute EWMA throughput
   *
   * @return the 15-minute EWMA throughput
   */
  public double get15mEWMAThp() {
    tickIfNecessary();
    return m15Thp.getRate(TimeUnit.SECONDS);
  }

  private long getTick() {
    return System.nanoTime();
  }

  private void tickIfNecessary() {
    final long oldTick = lastTick.get();
    final long newTick = getTick();
    final long age = newTick - oldTick;
    if (age > TICK_INTERVAL && lastTick.compareAndSet(oldTick, newTick)) {
      final long requiredTicks = age / TICK_INTERVAL;
      for (long i = 0; i < requiredTicks; i++) {
        m1Thp.tick();
        m5Thp.tick();
        m15Thp.tick();
      }
    }
  }
}
