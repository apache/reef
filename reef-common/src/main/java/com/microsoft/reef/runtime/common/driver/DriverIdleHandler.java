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
