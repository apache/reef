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
package org.apache.reef.runtime.common.driver;

import org.apache.reef.driver.parameters.DriverRestartHandler;
import org.apache.reef.driver.parameters.ServiceDriverRestartedHandlers;
import org.apache.reef.driver.restart.DriverRestartManager;
import org.apache.reef.driver.restart.DriverRestarted;
import org.apache.reef.exception.DriverFatalRuntimeException;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is bound to the start event of the clock and dispatches it to the appropriate application code.
 */
public final class DriverStartHandler implements EventHandler<StartTime> {

  private static final Logger LOG = Logger.getLogger(DriverStartHandler.class.getName());

  private final Set<EventHandler<StartTime>> startHandlers;
  private final Set<EventHandler<DriverRestarted>> restartHandlers;
  private final Set<EventHandler<DriverRestarted>> serviceRestartHandlers;

  private final DriverRestartManager driverRestartManager;

  @Inject
  private DriverStartHandler(
      @Parameter(org.apache.reef.driver.parameters.DriverStartHandler.class)
        final Set<EventHandler<StartTime>> startHandlers,
      @Parameter(DriverRestartHandler.class)
        final Set<EventHandler<DriverRestarted>> restartHandlers,
      @Parameter(ServiceDriverRestartedHandlers.class)
        final Set<EventHandler<DriverRestarted>> serviceRestartHandlers,
      final DriverRestartManager driverRestartManager) {

    this.startHandlers = startHandlers;
    this.restartHandlers = restartHandlers;
    this.serviceRestartHandlers = serviceRestartHandlers;
    this.driverRestartManager = driverRestartManager;

    LOG.log(Level.FINE,
        "Instantiated DriverStartHandler: StartHandlers:{0} RestartHandlers:{1} ServiceRestartHandlers:{2}",
        new Object[] {this.startHandlers, this.restartHandlers, this.serviceRestartHandlers});
  }

  @Override
  public void onNext(final StartTime startTime) {
    if (this.driverRestartManager.detectRestart()) {
      this.onRestart(startTime);
    } else {
      this.onStart(startTime);
    }
  }

  private void onRestart(final StartTime startTime) {

    if (this.restartHandlers.isEmpty()) {
      throw new DriverFatalRuntimeException("Driver restarted, but no ON_DRIVER_RESTART handler is bound.");
    }

    final List<EventHandler<DriverRestarted>> orderedRestartHandlers =
        new ArrayList<>(this.serviceRestartHandlers.size() + this.restartHandlers.size());

    orderedRestartHandlers.addAll(this.serviceRestartHandlers);
    orderedRestartHandlers.addAll(this.restartHandlers);

    // This can only be called after calling client restart handlers because REEF.NET
    // JobDriver requires making this call to set up the InterOp handlers.
    this.driverRestartManager.onRestart(startTime, orderedRestartHandlers);
  }

  private void onStart(final StartTime startTime) {
    for (final EventHandler<StartTime> startHandler : this.startHandlers) {
      startHandler.onNext(startTime);
    }
  }
}
