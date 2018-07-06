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

package org.apache.reef.bridge.driver.client;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.Clock;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver client exception handler.
 */
@Private
public final class DriverClientExceptionHandler implements EventHandler<Throwable> {
  private static final Logger LOG = Logger.getLogger(DriverClientExceptionHandler.class.getName());

  private final Clock clock;

  @Inject
  private DriverClientExceptionHandler(final Clock clock) {
    LOG.log(Level.FINE, "Instantiated 'DriverExceptionHandler'");
    this.clock = clock;
  }

  @Override
  public void onNext(final Throwable throwable) {
    LOG.log(Level.SEVERE, "Exception in DriverClient - stopping", throwable);
    this.clock.stop(throwable);
  }
}
