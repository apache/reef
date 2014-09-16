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
import com.microsoft.reef.runtime.common.driver.idle.DriverIdleManager;
import com.microsoft.reef.runtime.common.driver.idle.DriverIdlenessSource;
import com.microsoft.reef.runtime.common.driver.idle.IdleMessage;
import com.microsoft.tang.InjectionFuture;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the status of the Resource Manager.
 */
@DriverSide
@Private
public final class ResourceManagerStatus implements EventHandler<DriverRuntimeProtocol.RuntimeStatusProto>,
    DriverIdlenessSource {
  private static final Logger LOG = Logger.getLogger(ResourceManagerStatus.class.getName());

  private static final String COMPONENT_NAME = "ResourceManager";
  private static final IdleMessage IDLE_MESSAGE = new IdleMessage(COMPONENT_NAME, "No outstanding requests or allocations", true);

  private final ResourceManagerErrorHandler resourceManagerErrorHandler;
  private final DriverStatusManager driverStatusManager;
  private final InjectionFuture<DriverIdleManager> driverIdleManager;

  // Mutable state.
  private ReefServiceProtos.State state = ReefServiceProtos.State.INIT;
  private int outstandingContainerRequests = 0;
  private int containerAllocationCount = 0;

  @Inject
  ResourceManagerStatus(final ResourceManagerErrorHandler resourceManagerErrorHandler,
                        final DriverStatusManager driverStatusManager,
                        final InjectionFuture<DriverIdleManager> driverIdleManager) {
    this.resourceManagerErrorHandler = resourceManagerErrorHandler;
    this.driverStatusManager = driverStatusManager;
    this.driverIdleManager = driverIdleManager;
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
        this.onRMFailure(runtimeStatusProto);
        break;
      case DONE:
        this.onRMDone(runtimeStatusProto);
        break;
      case RUNNING:
        this.onRMRunning(runtimeStatusProto);
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
   * @return idle, if there are no outstanding requests or allocations. Not idle else.
   */
  @Override
  public synchronized IdleMessage getIdleStatus() {
    if (this.isIdle()) {
      return IDLE_MESSAGE;
    } else {
      final String message = new StringBuilder("There are ")
          .append(this.outstandingContainerRequests)
          .append(" outstanding container requests and ")
          .append(this.containerAllocationCount)
          .append(" allocated containers")
          .toString();
      return new IdleMessage(COMPONENT_NAME, message, false);
    }
  }


  private synchronized void onRMFailure(final DriverRuntimeProtocol.RuntimeStatusProto runtimeStatusProto) {
    assert (runtimeStatusProto.getState() == ReefServiceProtos.State.FAILED);
    this.resourceManagerErrorHandler.onNext(runtimeStatusProto.getError());
  }

  private synchronized void onRMDone(final DriverRuntimeProtocol.RuntimeStatusProto runtimeStatusProto) {
    assert (runtimeStatusProto.getState() == ReefServiceProtos.State.DONE);
    LOG.log(Level.INFO, "Resource Manager shutdown happened. Triggering Driver shutdown.");
    this.driverStatusManager.onComplete();
  }

  private synchronized void onRMRunning(final DriverRuntimeProtocol.RuntimeStatusProto runtimeStatusProto) {
    assert (runtimeStatusProto.getState() == ReefServiceProtos.State.RUNNING);
    if (this.isIdle()) {
      this.driverIdleManager.get().onPotentiallyIdle(IDLE_MESSAGE);
    }
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
