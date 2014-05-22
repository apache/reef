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
import com.microsoft.reef.runtime.common.driver.resourcemanager.ResourceManagerStatus;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.runtime.event.IdleClock;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Deals with the idle clock events: Checks whether we are done and if so closes the Driver.
 */
@Private
@DriverSide
public final class DriverIdleHandler implements EventHandler<IdleClock> {
  private static final Logger LOG = Logger.getLogger(DriverIdleHandler.class.getName());
  private final ResourceManagerStatus resourceManagerStatus;
  private final DriverStatusManager driverStatusManager;

  @Inject
  DriverIdleHandler(final ResourceManagerStatus resourceManagerStatus,
                    final DriverStatusManager driverStatusManager) {
    this.resourceManagerStatus = resourceManagerStatus;
    this.driverStatusManager = driverStatusManager;
  }

  @Override
  public synchronized void onNext(final IdleClock idleClock) {
    if (this.resourceManagerStatus.isRunningAndIdle()) {
      LOG.log(Level.INFO, "Idle resourcemanager shutdown");
      this.driverStatusManager.onComplete();
    }
  }
}
