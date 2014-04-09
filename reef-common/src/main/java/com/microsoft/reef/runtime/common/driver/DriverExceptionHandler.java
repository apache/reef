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
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;

/**
 * Exception handler for exceptions thrown by client code in the Driver.
 * It uses the JobMessageObserverImpl to send an update to the client and die.
 */
@Private
@DriverSide
public final class DriverExceptionHandler implements EventHandler<Throwable> {

  /**
   * We delegate the failures to this object.
   */
  private final DriverStatusManager driverStatusManager;

  @Inject
  public DriverExceptionHandler(final DriverStatusManager driverStatusManager) {
    this.driverStatusManager = driverStatusManager;
  }


  @Override
  public void onNext(final Throwable throwable) {
    this.driverStatusManager.onError(throwable);
  }
}
