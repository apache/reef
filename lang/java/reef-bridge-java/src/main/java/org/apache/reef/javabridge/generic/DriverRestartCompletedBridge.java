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
import org.apache.reef.annotations.audience.Interop;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.restart.DriverRestartCompleted;
import org.apache.reef.javabridge.NativeBridge;

/**
 * The Java-CLR bridge object for {@link DriverRestartCompleted} events.
 */
@Private
@DriverSide
@Unstable
@Interop(CppFiles = { "Clr2JavaImpl.h", "DriverRestartCompletedClr2Java.cpp" },
    CsFiles = { "IDriverRestartCompletedClr2Java.cs", "DriverRestartCompleted.cs" })
public final class DriverRestartCompletedBridge extends NativeBridge {
  private boolean isTimedOut;

  DriverRestartCompletedBridge(final DriverRestartCompleted driverRestartCompleted) {
    this.isTimedOut = driverRestartCompleted.isTimedOut();
  }

  public boolean isTimedOut() {
    return isTimedOut;
  }

  @Override
  public void close() throws Exception {
  }
}
