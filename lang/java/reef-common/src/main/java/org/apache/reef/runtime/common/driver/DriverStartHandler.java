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
package org.apache.reef.runtime.common.driver;

import org.apache.reef.driver.parameters.DriverRestartHandler;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is bound to the start event of the clock and dispatches it to the approriate application code.
 */
public final class DriverStartHandler implements EventHandler<StartTime> {
  private static final Logger LOG = Logger.getLogger(DriverStartHandler.class.getName());

  private final Set<EventHandler<StartTime>> startHandlers;
  private final Optional<EventHandler<StartTime>> restartHandler;
  private final DriverStatusManager driverStatusManager;

  @Inject
  DriverStartHandler(final @Parameter(org.apache.reef.driver.parameters.DriverStartHandler.class) Set<EventHandler<StartTime>> startHandler,
                     final @Parameter(DriverRestartHandler.class) EventHandler<StartTime> restartHandler,
                     final DriverStatusManager driverStatusManager) {
    this.startHandlers = startHandler;
    this.restartHandler = Optional.of(restartHandler);
    this.driverStatusManager = driverStatusManager;
    LOG.log(Level.FINE, "Instantiated `DriverStartHandler with StartHandler [{0}] and RestartHandler [{1}]",
        new String[]{this.startHandlers.toString(), this.restartHandler.toString()});
  }

  @Inject
  DriverStartHandler(final @Parameter(org.apache.reef.driver.parameters.DriverStartHandler.class) Set<EventHandler<StartTime>> startHandler,
                     final DriverStatusManager driverStatusManager) {
    this.startHandlers = startHandler;
    this.restartHandler = Optional.empty();
    this.driverStatusManager = driverStatusManager;
    LOG.log(Level.FINE, "Instantiated `DriverStartHandler with StartHandler [{0}] and no RestartHandler",
        this.startHandlers.toString());
  }

  @Override
  public void onNext(final StartTime startTime) {
    if (isRestart()) {
      this.onRestart(startTime);
    } else {
      this.onStart(startTime);
    }
  }

  private void onRestart(final StartTime startTime) {
    if (restartHandler.isPresent()) {
      this.restartHandler.get().onNext(startTime);
    } else {
      // TODO: We might have to indicate this to YARN somehow such that it doesn't try another time.
      throw new RuntimeException("Driver restart happened, but no ON_DRIVER_RESTART handler is bound.");
    }
  }

  private void onStart(final StartTime startTime) {
    for (final EventHandler<StartTime> startHandler : this.startHandlers) {
      startHandler.onNext(startTime);
    }
  }

  /**
   * @return true, if the Driver is in fact being restarted.
   */
  private boolean isRestart() {
    return this.driverStatusManager.getNumPreviousContainers() > 0;
  }
}
