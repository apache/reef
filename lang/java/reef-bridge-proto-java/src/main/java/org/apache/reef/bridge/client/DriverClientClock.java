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

package org.apache.reef.bridge.client;

import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.Time;
import org.apache.reef.wake.time.event.Alarm;

import javax.inject.Inject;

/**
 * The bridge driver client clock.
 */
public final class DriverClientClock implements Clock {

  private boolean closed = false;

  @Inject
  private DriverClientClock() {

  }

  @Override
  public Time scheduleAlarm(final int offset, final EventHandler<Alarm> handler) {
    return null;
  }

  @Override
  public void close() {
    this.closed = true;
  }

  @Override
  public void stop() {

  }

  @Override
  public void stop(final Throwable exception) {

  }

  @Override
  public boolean isIdle() {
    return this.closed;
  }

  @Override
  public boolean isClosed() {
    return this.closed;
  }

  @Override
  public void run() {

  }
}
