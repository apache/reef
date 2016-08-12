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
package org.apache.reef.wake.time.runtime;

import org.apache.reef.wake.time.Time;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Logical timer that is only bound to the timestamps of the events tracked against it.
 * In such setting, all events occur immediately, i.e. isReady() always return true,
 * and the duration of the delay to the next event is always 0.
 * Current time for this timer is always the timestamp of the tracked event that is
 * the most distant in the future.
 */
public final class LogicalTimer implements Timer {

  /**
   * Current time in milliseconds since the beginning of the epoch (01/01/1970),
   * according to the timer. For this implementation, always keep the largest seen
   * timestamp (i.e. track the event that is the most distant into the future).
   */
  private final AtomicLong current = new AtomicLong(0);

  /**
   * Instances of the timer should only be created automatically by Tang.
   */
  @Inject
  private LogicalTimer() {
  }

  /**
   * Get current time in milliseconds since the beginning of the epoch (01/01/1970).
   * This timer implementation always returns the timestamp of the most distant
   * future event ever checked against this timer in getDuration() or isReady() methods.
   * Return 0 if there were no calls yet to getDuration() or isReady().
   * @return Timestamp of the latest event (in milliseconds since the start of the epoch).
   */
  @Override
  public long getCurrent() {
    return this.current.get();
  }

  /**
   * Get the number of milliseconds between current time as tracked by the Timer implementation
   * and a given event. This implementation always returns 0 and updates current timer's time
   * to the timestamp of the most distant future event.
   * @param time Timestamp object that wraps time in milliseconds.
   * @return Always returns 0.
   */
  @Override
  public long getDuration(final Time time) {
    this.isReady(time);
    return 0;
  }

  /**
   * Check if the event with a given timestamp has occurred, according to the timer.
   * This implementation always returns true and updates current timer's time to the timestamp
   * of the most distant future event.
   * @param time Timestamp object that wraps time in milliseconds.
   * @return Always returns true.
   */
  @Override
  public boolean isReady(final Time time) {
    for (;;) {
      final long otherTs = time.getTimestamp();
      final long thisTs = this.current.get();
      if (thisTs >= otherTs || this.current.compareAndSet(thisTs, otherTs)) {
        return true;
      }
    }
  }
}
