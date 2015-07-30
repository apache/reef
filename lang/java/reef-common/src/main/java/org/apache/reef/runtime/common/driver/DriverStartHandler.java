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
import org.apache.reef.driver.restart.RestartEvaluatorInfo;
import org.apache.reef.exception.DriverFatalRuntimeException;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is bound to the start event of the clock and dispatches it to the appropriate application code.
 */
public final class DriverStartHandler implements EventHandler<StartTime> {
  private static final Logger LOG = Logger.getLogger(DriverStartHandler.class.getName());

  private final Set<EventHandler<StartTime>> startHandlers;
  private final Optional<Set<EventHandler<StartTime>>> restartHandlers;
  private final Optional<Set<EventHandler<StartTime>>> serviceRestartHandlers;
  private final Optional<DriverRestartManager> driverRestartManager;

  @Inject
  DriverStartHandler(@Parameter(org.apache.reef.driver.parameters.DriverStartHandler.class)
                     final Set<EventHandler<StartTime>> startHandler,
                     @Parameter(DriverRestartHandler.class)
                     final Set<EventHandler<StartTime>> restartHandlers,
                     @Parameter(ServiceDriverRestartedHandlers.class)
                     final Set<EventHandler<StartTime>> serviceRestartHandlers,
                     final DriverRestartManager driverRestartManager,
                     final DriverStatusManager driverStatusManager) {
    this(startHandler, Optional.of(restartHandlers), Optional.of(serviceRestartHandlers),
        Optional.of(driverRestartManager), driverStatusManager);
    LOG.log(Level.FINE, "Instantiated `DriverStartHandler with StartHandlers [{0}], RestartHandlers [{1}]," +
            "and ServiceRestartHandlers [{2}], with a restart manager.",
        new String[] {this.startHandlers.toString(), this.restartHandlers.toString(),
            this.serviceRestartHandlers.toString()});
  }

  @Inject
  DriverStartHandler(@Parameter(org.apache.reef.driver.parameters.DriverStartHandler.class)
                     final Set<EventHandler<StartTime>> startHandlers,
                     @Parameter(DriverRestartHandler.class)
                     final Set<EventHandler<StartTime>> restartHandlers,
                     final DriverRestartManager driverRestartManager,
                     final DriverStatusManager driverStatusManager) {
    this(startHandlers, Optional.of(restartHandlers), Optional.<Set<EventHandler<StartTime>>>empty(),
        Optional.of(driverRestartManager), driverStatusManager);
    LOG.log(Level.FINE, "Instantiated `DriverStartHandler with StartHandlers [{0}], RestartHandlers [{1}]," +
            " with a restart manager.",
        new String[] {this.startHandlers.toString(), this.restartHandlers.toString()});
  }

  @Inject
  DriverStartHandler(@Parameter(org.apache.reef.driver.parameters.DriverStartHandler.class)
                     final Set<EventHandler<StartTime>> startHandlers,
                     final DriverStatusManager driverStatusManager) {
    this(startHandlers, Optional.<Set<EventHandler<StartTime>>>empty(),
        Optional.<Set<EventHandler<StartTime>>>empty(), Optional.<DriverRestartManager>empty(), driverStatusManager);
    LOG.log(Level.FINE, "Instantiated `DriverStartHandler with StartHandlers [{0}] and no restart.",
        this.startHandlers.toString());
  }

  private DriverStartHandler(final Set<EventHandler<StartTime>> startHandler,
                             final Optional<Set<EventHandler<StartTime>>> restartHandlers,
                             final Optional<Set<EventHandler<StartTime>>> serviceRestartHandlers,
                             final Optional<DriverRestartManager> driverRestartManager,
                             final DriverStatusManager driverStatusManager) {
    this.startHandlers = startHandler;
    this.restartHandlers = restartHandlers;
    this.serviceRestartHandlers = serviceRestartHandlers;
    this.driverRestartManager = driverRestartManager;
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
    if (this.driverRestartManager.isPresent() && this.restartHandlers.isPresent()) {
      for (EventHandler<StartTime> serviceRestartHandler : this.serviceRestartHandlers.get()) {
        serviceRestartHandler.onNext(startTime);
      }

      for (EventHandler<StartTime> restartHandler : this.restartHandlers.get()){
        restartHandler.onNext(startTime);
      }

      // This can only be called after calling client restart handlers because REEF.NET
      // JobDriver requires making this call to set up the InterOp handlers.
      final RestartEvaluatorInfo restartEvaluatorInfo = this.driverRestartManager.get().onRestartRecoverEvaluators();
      this.driverRestartManager.get().informAboutEvaluatorAlive(restartEvaluatorInfo.getRecoveredEvaluatorIds());
      this.driverRestartManager.get().informAboutEvaluatorFailures(restartEvaluatorInfo.getFailedEvaluatorIds());
    } else {
      throw new DriverFatalRuntimeException("Driver restart happened, but no ON_DRIVER_RESTART handler is bound.");
    }
  }

  private void onStart(final StartTime startTime) {
    for (final EventHandler<StartTime> startHandler : this.startHandlers) {
      startHandler.onNext(startTime);
    }
  }

  /**
   * @return true, if the configurations enable restart and the Driver is in fact being restarted.
   */
  private boolean isRestart() {
    if (this.driverRestartManager.isPresent()) {
      return this.driverRestartManager.get().isRestart();
    }

    return false;
  }
}
