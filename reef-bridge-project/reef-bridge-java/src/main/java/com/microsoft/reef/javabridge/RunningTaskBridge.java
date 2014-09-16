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

package com.microsoft.reef.javabridge;

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.task.RunningTask;

import java.util.logging.Logger;

public class RunningTaskBridge extends NativeBridge {
  private static final Logger LOG = Logger.getLogger(RunningTaskBridge.class.getName());

  final private RunningTask jrunningTask;
  final private ActiveContextBridge jactiveContext;

  public RunningTaskBridge(RunningTask runningTask) {
    jrunningTask = runningTask;
    final ActiveContext activeContext = runningTask.getActiveContext();
    jactiveContext = new ActiveContextBridge(activeContext);
  }

  public final String getId() {
    return jrunningTask.getId();
  }

  public final void send(final byte[] message) {
    jrunningTask.send(message);
  }

  @Override
  public void close() {
  }
}
