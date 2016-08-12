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
package org.apache.reef.wake.time;

import java.util.Date;

/**
 * An abstract object that has a timestamp.
 * That allows us to compare and order objects by the time.
 */
public abstract class Time implements Comparable<Time> {

  private final long timestamp;

  /**
   * Initialize the internal timestamp. Timestamp remains constant
   * for the entire lifecycle of the object.
   * @param timestamp timestamp in milliseconds since the beginning
   * of the epoch (01/01/1970).
   */
  public Time(final long timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * Get timestamp in milliseconds since the beginning of the epoch (01/01/1970).
   * @return Object's timestamp in milliseconds since the start of the epoch.
   */
  public final long getTimestamp() {
    return this.timestamp;
  }

  /**
   * Get timestamp in milliseconds since the beginning of the epoch (01/01/1970).
   * @return Object's timestamp in milliseconds since the start of the epoch.
   * @deprecated [REEF-1532] Prefer using getTimestamp() instead.
   * Remove after release 0.16.
   */
  public final long getTimeStamp() {
    return this.timestamp;
  }

  @Override
  public String toString() {
    return this.getClass().getName()
        + ":[" + this.timestamp + '|' + new Date(this.timestamp) + ']';
  }

  @Override
  public int compareTo(final Time other) {
    final int cmp = Long.compare(this.timestamp, other.timestamp);
    return cmp != 0 ? cmp : Integer.compare(this.hashCode(), other.hashCode());
  }

  @Override
  public boolean equals(final Object other) {
    return other instanceof Time && compareTo((Time) other) == 0;
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
