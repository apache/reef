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

import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.wake.time.Time;

/**
 * An interface for Timer.
 * Default implementation uses actual system time.
 */
@DefaultImplementation(RealTimer.class)
public interface Timer {

  /**
   * Get current time in milliseconds since the beginning of the epoch (01/01/1970).
   * Note that this time may not necessarily match the actual system time - e.g. in unit tests.
   * @return Current system time in milliseconds since the start of the epoch.
   */
  long getCurrent();

  /**
   * Get the number of milliseconds between current time as tracked by the Timer implementation
   * and the given event. Can return a negative number if the event is already in the past.
   * @param time Timestamp object that wraps time in milliseconds.
   * @return Difference in milliseconds between the given timestamp and the time tracked by the timer.
   * The result is a negative number if the timestamp is in the past (according to the timer's time).
   */
  long getDuration(final Time time);

  /**
   * Check if the event with a given timestamp has occurred, according to the timer.
   * Return true if the timestamp is equal or less than the timer's time, and false if
   * it is still in the (timer's) future.
   * @param time Timestamp object that wraps time in milliseconds.
   * @return False if the given timestamp is still in the timer's time future.
   */
  boolean isReady(final Time time);
}
