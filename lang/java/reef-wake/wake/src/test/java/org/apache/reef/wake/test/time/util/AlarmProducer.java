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
package org.apache.reef.wake.test.time.util;

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.runtime.RuntimeClock;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Helper class used in unit tests to generate alarms at user-specified intervals
 * and count down the barrier on each alarm. It is used in RuntimeClockTest.
 */
public abstract class AlarmProducer implements EventHandler<Alarm> {

  private final RuntimeClock clock;
  private final CountDownLatch eventCountLatch;

  /**
   * Create a new alarm producer.
   * @param clock Event loop that processes the schedule and invokes alarm handlers.
   * @param latch A barrier with the counter that gets decremented after each alarm.
   */
  public AlarmProducer(final RuntimeClock clock, final CountDownLatch latch) {
    this.clock = clock;
    this.eventCountLatch = latch;
  }

  /**
   * On each alarm, schedule the next one and decrement the latch.
   * @param value An alarm to process.
   */
  @Override
  public void onNext(final Alarm value) {
    this.clock.scheduleAlarm(this.getOffset(), this);
    this.eventCountLatch.countDown();
  }

  /**
   * Return offset from current time in milliseconds for when to schedule an alarm.
   * @return Time offset in milliseconds. Must be a positive number.
   */
  public abstract int getOffset();

  /**
   * Generate random integer uniformly distributed between offsetMin and offsetMax.
   * Helper function to be used in getOffset().
   * @param rand Random number generator, and instance of Random.
   * @param offsetMin Lower bound, must be > 0.
   * @param offsetMax Upper bound, must be > offsetMin.
   * @return Random integer uniformly distributed between offsetMin and offsetMax.
   */
  public static int randomOffsetUniform(final Random rand, final int offsetMin, final int offsetMax) {
    assert offsetMin > 0;
    assert offsetMin <= offsetMax;
    return rand.nextInt(offsetMax - offsetMin + 1) + offsetMin;
  }

  /**
   * Generate random integer drawn from Poisson distribution.
   * Helper function to be used in getOffset(). Always returns a positive integer.
   * We use normal distribution with mu=lambda and sigma=sqrt(lambda) to approximate
   * the Poisson distribution.
   * @param rand Random number generator, and instance of Random.
   * @param lambda Parameter of the Poisson distribution, must be > 0.
   * @return A rounded absolute value of a random number drawn from a Poisson distribution.
   */
  public static int randomOffsetPoisson(final Random rand, final double lambda) {
    assert lambda > 0;
    return (int)Math.round(Math.abs(rand.nextGaussian() * Math.sqrt(lambda) + lambda) + 1);
  }
}
