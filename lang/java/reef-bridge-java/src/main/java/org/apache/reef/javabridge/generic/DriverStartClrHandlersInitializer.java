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
import org.apache.reef.javabridge.BridgeHandlerManager;
import org.apache.reef.javabridge.EvaluatorRequestorBridge;
import org.apache.reef.javabridge.NativeInterop;
import org.apache.reef.wake.time.event.StartTime;

/**
 * An initializer implementation that initializes ClrHandlers for the CLR {@link JobDriver} for the case
 * of regular Driver startup.
 */
@Private
@DriverSide
@Unstable
final class DriverStartClrHandlersInitializer implements ClrHandlersInitializer {
  private final StartTime startTime;

  DriverStartClrHandlersInitializer(final StartTime startTime) {
    this.startTime = startTime;
  }

  @Override
  public BridgeHandlerManager getClrHandlers(final String portNumber,
                                             final EvaluatorRequestorBridge evaluatorRequestorBridge) {
    BridgeHandlerManager bridgeHandlerManager = new BridgeHandlerManager();
    NativeInterop.callClrSystemOnStartHandler(startTime.toString(), portNumber, bridgeHandlerManager,
        evaluatorRequestorBridge);

    return bridgeHandlerManager;
  }
}
