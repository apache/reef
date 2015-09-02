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
package org.apache.reef.javabridge.generic;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.restart.DriverRestarted;
import org.apache.reef.javabridge.DriverRestartedBridge;
import org.apache.reef.javabridge.EvaluatorRequestorBridge;
import org.apache.reef.javabridge.NativeInterop;

/**
 * An initializer implementation that initializes ClrHandlers for the CLR {@link JobDriver} for the case
 * where the Driver has restarted.
 */
@Private
@DriverSide
@Unstable
final class DriverRestartClrHandlersInitializer implements ClrHandlersInitializer {
  private final DriverRestarted driverRestarted;

  DriverRestartClrHandlersInitializer(final DriverRestarted driverRestarted) {
    this.driverRestarted = driverRestarted;
  }

  @Override
  public long[] getClrHandlers(final String portNumber, final EvaluatorRequestorBridge evaluatorRequestorBridge) {
    // TODO[REEF-689]: Make callClrSystemOnRestartedHandlerOnNext take DriverRestarted object.
    return NativeInterop.callClrSystemOnRestartHandlerOnNext(
        portNumber,
        evaluatorRequestorBridge, new DriverRestartedBridge(driverRestarted.getExpectedEvaluatorIds()));
  }
}
