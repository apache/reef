/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.reef.mock;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.Time;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.runtime.event.ClientAlarm;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The MockClock can be used to drive alarms set by the client application.
 */
@Private
public final class MockClock implements Clock {

  private final MockRuntime runtime;

  private final List<Alarm> alarmList = new ArrayList<>();

  private long currentTime = 0;

  private boolean closed = false;

  @Inject
  MockClock(final MockRuntime runtime) {
    this.runtime = runtime;
  }

  /**
   * Advances the clock by the offset amount.
   * @param offset amount to advance clock
   */
  public void advanceClock(final int offset) {
    this.currentTime += offset;
    final Iterator<Alarm> iter = this.alarmList.iterator();
    while (iter.hasNext()) {
      final Alarm alarm = iter.next();
      if (alarm.getTimestamp() <= this.currentTime) {
        alarm.run();
        iter.remove();
      }
    }
  }

  /**
   * @return the current mock clock time
   */
  public long getCurrentTime() {
    return this.currentTime;
  }

  @Override
  public Time scheduleAlarm(final int offset, final EventHandler<Alarm> handler) {
    final Alarm alarm = new ClientAlarm(this.currentTime + offset, handler);
    alarmList.add(alarm);
    return alarm;
  }

  @Override
  public void close() {
    if (!closed) {
      this.runtime.stop();
      this.closed = true;
    }
  }

  @Override
  public void stop() {
    close();
  }

  @Override
  public void stop(final Throwable exception) {
    close();
  }

  @Override
  public boolean isIdle() {
    return this.alarmList.size() > 0;
  }

  @Override
  public boolean isClosed() {
    return this.closed;
  }

  @Override
  public void run() {
    this.runtime.start();
  }
}
