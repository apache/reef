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
package org.apache.reef.runtime.common.driver.idle;

import org.apache.reef.driver.parameters.DriverIdleSources;
import org.apache.reef.runtime.common.driver.DriverStatusManager;
import org.apache.reef.tang.InjectionFuture;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles the various sources for driver idleness and forwards decisions to DriverStatusManager.
 */
public final class DriverIdleManager {
  private static final Logger LOG = Logger.getLogger(DriverIdleManager.class.getName());
  private static final Level IDLE_REASONS_LEVEL = Level.FINEST;
  private final Set<DriverIdlenessSource> idlenessSources;
  private final InjectionFuture<DriverStatusManager> driverStatusManager;

  @Inject
  DriverIdleManager(final @Parameter(DriverIdleSources.class) Set<DriverIdlenessSource> idlenessSources,
                    final InjectionFuture<DriverStatusManager> driverStatusManager) {
    this.idlenessSources = idlenessSources;
    this.driverStatusManager = driverStatusManager;
  }

  public synchronized void onPotentiallyIdle(final IdleMessage reason) {
    synchronized (driverStatusManager.get()) {
      if (this.driverStatusManager.get().isShuttingDownOrFailing()) {
        LOG.log(IDLE_REASONS_LEVEL, "Ignoring idle call from [{0}] for reason [{1}]",
            new Object[]{reason.getComponentName(), reason.getReason()});
      } else {
        boolean isIdle = true;
        LOG.log(IDLE_REASONS_LEVEL, "Checking for idle because {0} reported idleness for reason [{1}]",
            new Object[]{reason.getComponentName(), reason.getReason()});


        for (final DriverIdlenessSource idlenessSource : this.idlenessSources) {
          final IdleMessage idleMessage = idlenessSource.getIdleStatus();
          LOG.log(IDLE_REASONS_LEVEL, "[{0}] is reporting {1} because [{2}]."
              , new Object[]{idleMessage.getComponentName(), idleMessage.isIdle() ? "idle" : "not idle", idleMessage.getReason()}
          );
          isIdle &= idleMessage.isIdle();
        }

        if (isIdle) {
          LOG.log(Level.INFO, "All components indicated idle. Initiating Driver shutdown.");
          this.driverStatusManager.get().onComplete();
        }
      }
    }
  }
}
