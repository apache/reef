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
package org.apache.reef.javabridge;

import org.apache.reef.annotations.audience.Interop;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.io.naming.Identifiable;

import java.util.logging.Logger;

/**
 * The Java-CLR bridge object for {@link org.apache.reef.driver.task.RunningTask}.
 */
@Private
@Interop(
    CppFiles = { "Clr2JavaImpl.h", "RunningTaskClr2Java.cpp" },
    CsFiles = { "IRunningTaskClr2Java.cs", "RunningTask.cs" })
public final class RunningTaskBridge extends NativeBridge implements Identifiable {
  private static final Logger LOG = Logger.getLogger(RunningTaskBridge.class.getName());

  private final RunningTask jrunningTask;
  private final ActiveContextBridge jactiveContext;

  public RunningTaskBridge(final RunningTask runningTask, final ActiveContextBridgeFactory factory) {
    this.jrunningTask = runningTask;
    this.jactiveContext = factory.getActiveContextBridge(runningTask.getActiveContext());
  }

  public void send(final byte[] message) {
    jrunningTask.send(message);
  }

  public ActiveContextBridge getActiveContext() {
    return jactiveContext;
  }

  @Override
  public String getId() {
    return jrunningTask.getId();
  }

  @Override
  public void close() {
  }
}
