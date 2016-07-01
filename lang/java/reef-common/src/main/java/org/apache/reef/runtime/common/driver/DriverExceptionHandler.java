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
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Exception handler for exceptions thrown by client code in the Driver.
 * It uses the JobMessageObserverImpl to send an update to the client and die.
 */
@Private
@DriverSide
public final class DriverExceptionHandler implements EventHandler<Throwable> {
  private static final Logger LOG = Logger.getLogger(DriverExceptionHandler.class.getName());
  /**
   * We delegate the failures to this object.
   */
  private final DriverStatusManager driverStatusManager;

  @Inject
  public DriverExceptionHandler(final DriverStatusManager driverStatusManager) {
    this.driverStatusManager = driverStatusManager;
    LOG.log(Level.FINE, "Instantiated 'DriverExceptionHandler'");
  }


  @Override
  public void onNext(final Throwable throwable) {
    this.driverStatusManager.onError(throwable);
  }
}

