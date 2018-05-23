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

package org.apache.reef.bridge.driver.client;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;
import org.apache.reef.wake.time.Time;
import org.apache.reef.wake.time.event.Alarm;
import org.apache.reef.wake.time.runtime.Timer;
import org.apache.reef.wake.time.runtime.event.ClientAlarm;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The bridge driver client clock.
 */
@Private
public final class DriverClientClock implements Clock, IAlarmDispatchHandler {

  private static final Logger LOG = Logger.getLogger(DriverClientClock.class.getName());

  private final IDriverClientService driverClientService;

  private final IDriverServiceClient driverServiceClient;

  private final Timer timer;

  private final Map<String, ClientAlarm> alarmMap = new HashMap<>();

  private boolean closed = false;

  @Inject
  private DriverClientClock(
      final Timer timer,
      final IDriverClientService driverClientService,
      final IDriverServiceClient driverServiceClient) {
    this.timer = timer;
    this.driverClientService = driverClientService;
    this.driverServiceClient = driverServiceClient;
  }

  @Override
  public Time scheduleAlarm(final int offset, final EventHandler<Alarm> handler) {
    LOG.log(Level.INFO, "Schedule alarm offset {0}", offset);
    final ClientAlarm alarm = new ClientAlarm(this.timer.getCurrent() + offset, handler);
    final String alarmId = UUID.randomUUID().toString();
    this.alarmMap.put(alarmId, alarm);
    this.driverServiceClient.onSetAlarm(alarmId, offset);
    LOG.log(Level.INFO, "Alarm {0} scheduled at offset {1}", new Object[]{alarmId, offset});
    return alarm;
  }

  @Override
  public void close() {
    stop();
  }

  @Override
  public void stop() {
    if (!closed) {
      this.closed = true;
      this.driverServiceClient.onShutdown();
    }
  }

  @Override
  public void stop(final Throwable exception) {
    if (!closed) {
      this.closed = true;
      this.driverServiceClient.onShutdown(exception);
    }
  }

  @Override
  public boolean isIdle() {
    return this.closed || this.alarmMap.isEmpty();
  }

  @Override
  public boolean isClosed() {
    return this.closed;
  }

  @Override
  public void run() {
    try {
      this.driverClientService.start();
      this.driverClientService.awaitTermination();
    } catch (final IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Alarm clock event handler.
   * @param alarmId alarm identifier
   */
  @Override
  public void onNext(final String alarmId) {
    LOG.log(Level.INFO, "Alarm {0} triggered", alarmId);
    final ClientAlarm clientAlarm = this.alarmMap.remove(alarmId);
    if (clientAlarm != null) {
      clientAlarm.run();
    } else {
      LOG.log(Level.SEVERE, "Unknown alarm id {0}", alarmId);
    }
  }
}
