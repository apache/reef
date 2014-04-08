/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.common.driver;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.runtime.common.driver.evaluator.Evaluators;
import com.microsoft.reef.runtime.common.utils.RemoteManager;
import com.microsoft.reef.util.Optional;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.runtime.event.RuntimeStop;

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

  private final DriverStatusManager driverStatusManager;
  private final RemoteManager remoteManager;
  private final Evaluators evaluators;

  @Inject
  DriverRuntimeStopHandler(final DriverStatusManager driverStatusManager,
                           final RemoteManager remoteManager,
                           final Evaluators evaluators) {
    this.driverStatusManager = driverStatusManager;
    this.remoteManager = remoteManager;
    this.evaluators = evaluators;
  }

  @Override
  public synchronized void onNext(final RuntimeStop runtimeStop) {
    LOG.log(Level.FINEST, "RuntimeStop: {0}", runtimeStop);
    // Shutdown the Evaluators.
    this.evaluators.close();
    // Inform the client of the shutdown.
    final Optional<Throwable> exception = Optional.<Throwable>ofNullable(runtimeStop.getException());
    this.driverStatusManager.sendJobEndingMessageToClient(exception);
    // Close the remoteManager.
    try {
      this.remoteManager.close();
      LOG.log(Level.INFO, "Driver shutdown complete");
    } catch (final Exception e) {
      throw new RuntimeException("Unable to close the RemoteManager.", e);
    }
  }
}
