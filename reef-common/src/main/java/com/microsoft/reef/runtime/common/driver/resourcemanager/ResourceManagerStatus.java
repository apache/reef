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
package com.microsoft.reef.runtime.common.driver.resourcemanager;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.proto.ReefServiceProtos;
import com.microsoft.reef.runtime.common.driver.DriverStatusManager;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the status of the Resource Manager.
 */
@DriverSide
@Private
public final class ResourceManagerStatus implements EventHandler<DriverRuntimeProtocol.RuntimeStatusProto> {
  private static final Logger LOG = Logger.getLogger(ResourceManagerStatus.class.getName());
  private final String name = "REEF";
  private final ResourceManagerErrorHandler resourceManagerErrorHandler;
  private final DriverStatusManager driverStatusManager;

  // Mutable state.
  private ReefServiceProtos.State state = ReefServiceProtos.State.INIT;
  private int outstandingContainerRequests = 0;
  private int containerAllocationCount = 0;

  @Inject
  ResourceManagerStatus(final ResourceManagerErrorHandler resourceManagerErrorHandler,
                        final DriverStatusManager driverStatusManager) {
    this.resourceManagerErrorHandler = resourceManagerErrorHandler;
    this.driverStatusManager = driverStatusManager;
  }

  @Override
  public synchronized void onNext(final DriverRuntimeProtocol.RuntimeStatusProto runtimeStatusProto) {
    final ReefServiceProtos.State newState = runtimeStatusProto.getState();
    LOG.log(Level.FINEST, "Runtime status " + runtimeStatusProto);
    this.outstandingContainerRequests = runtimeStatusProto.getOutstandingContainerRequests();
    this.containerAllocationCount = runtimeStatusProto.getContainerAllocationCount();
    this.setState(runtimeStatusProto.getState());

    switch (newState) {
      case FAILED:
        this.resourceManagerErrorHandler.onNext(runtimeStatusProto.getError());
        break;
      case DONE:
        LOG.log(Level.INFO, "Resource Manager shutdown happened. Triggering Driver shutdown.");
        this.driverStatusManager.onComplete();
        break;
      case RUNNING:
        break;
    }
  }

  /**
   * Change the state of the Resource Manager to be RUNNING.
   */
  public synchronized void setRunning() {
    this.setState(ReefServiceProtos.State.RUNNING);
  }

  /**
   * @return whether the resource manager is running and idle.
   */
  public synchronized boolean isRunningAndIdle() {
    return isRunning() && isIdle();
  }

  private synchronized boolean isIdle() {
    return this.hasNoOutstandingRequests()
        && this.hasNoContainersAllocated();
  }

  private synchronized boolean isRunning() {
    return ReefServiceProtos.State.RUNNING.equals(this.state);
  }


  private synchronized void setState(ReefServiceProtos.State state) {
    // TODO: Add state transition check
    this.state = state;
  }


  private synchronized boolean hasNoOutstandingRequests() {
    return this.outstandingContainerRequests == 0;
  }

  private synchronized boolean hasNoContainersAllocated() {
    return this.containerAllocationCount == 0;
  }

}
