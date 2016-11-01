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

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.parameters.ResourceManagerPreserveEvaluators;
import org.apache.reef.driver.restart.DriverRestartManager;
import org.apache.reef.exception.DriverFatalRuntimeException;
import org.apache.reef.runtime.common.driver.api.ResourceManagerStopHandler;
import org.apache.reef.runtime.common.driver.evaluator.EvaluatorIdlenessThreadPool;
import org.apache.reef.runtime.common.driver.evaluator.Evaluators;
import org.apache.reef.runtime.common.utils.RemoteManager;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.runtime.event.RuntimeStop;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handler for the RuntimeStop event in the Driver. It shuts down the  evaluators and the RemoteManager and
 * informs the Client.
 */
@Private
@DriverSide
final class DriverRuntimeStopHandler implements EventHandler<RuntimeStop> {

  private static final Logger LOG = Logger.getLogger(DriverRuntimeStopHandler.class.getName());

  private final DriverRestartManager driverRestartManager;
  private final DriverStatusManager driverStatusManager;
  private final ResourceManagerStopHandler resourceManagerStopHandler;
  private final RemoteManager remoteManager;
  private final Evaluators evaluators;
  private final EvaluatorIdlenessThreadPool idlenessChecker;
  private final boolean preserveEvaluatorsAcrossRestarts;

  @Inject
  private DriverRuntimeStopHandler(
      @Parameter(ResourceManagerPreserveEvaluators.class) final boolean preserveEvaluatorsAcrossRestarts,
      final DriverRestartManager driverRestartManager,
      final DriverStatusManager driverStatusManager,
      final ResourceManagerStopHandler resourceManagerStopHandler,
      final RemoteManager remoteManager,
      final Evaluators evaluators,
      final EvaluatorIdlenessThreadPool idlenessChecker) {

    this.driverRestartManager = driverRestartManager;
    this.driverStatusManager = driverStatusManager;
    this.resourceManagerStopHandler = resourceManagerStopHandler;
    this.remoteManager = remoteManager;
    this.evaluators = evaluators;
    this.idlenessChecker = idlenessChecker;
    this.preserveEvaluatorsAcrossRestarts = preserveEvaluatorsAcrossRestarts;
  }

  @Override
  public synchronized void onNext(final RuntimeStop runtimeStop) {

    LOG.log(Level.FINE, "Driver shutdown: start {0}", runtimeStop);

    final Throwable runtimeException = runtimeStop.getException();

    // Shut down evaluators if there are no exceptions, the driver is forcefully
    // shut down by a non-recoverable exception, or restart is not enabled.
    if (runtimeException == null ||
        runtimeException instanceof DriverFatalRuntimeException ||
        !this.preserveEvaluatorsAcrossRestarts) {
      LOG.log(Level.FINER, "Driver shutdown: close the evaluators");
      this.evaluators.close();
    }

    this.resourceManagerStopHandler.onNext(runtimeStop);

    LOG.log(Level.FINER, "Driver shutdown: notify the client");
    this.driverStatusManager.onRuntimeStop(Optional.ofNullable(runtimeException));

    try {
      LOG.log(Level.FINER, "Driver shutdown: close the remote manager");
      this.remoteManager.close();
    } catch (final Exception e) {
      LOG.log(Level.WARNING, "Error when closing the RemoteManager", e);
      throw new RuntimeException("Unable to close the RemoteManager.", e);
    }

    LOG.log(Level.FINER, "Driver shutdown: close the restart manager");
    this.driverRestartManager.close();

    LOG.log(Level.FINER, "Driver shutdown: close the idleness checker");
    this.idlenessChecker.close();

    LOG.log(Level.INFO, "Driver shutdown complete");
  }
}
