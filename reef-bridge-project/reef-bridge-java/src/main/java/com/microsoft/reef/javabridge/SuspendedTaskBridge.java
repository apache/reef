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

import com.microsoft.reef.driver.task.SuspendedTask;
import com.microsoft.reef.io.Message;
import com.microsoft.reef.io.naming.Identifiable;

public class SuspendedTaskBridge extends NativeBridge implements Identifiable, Message {

  private final SuspendedTask jsuspendedTask;
  private final String taskId;
  private final ActiveContextBridge jactiveContext;

  public SuspendedTaskBridge(SuspendedTask suspendedTask) {
    jsuspendedTask = suspendedTask;
    taskId = suspendedTask.getId();
    jactiveContext =  new ActiveContextBridge(jsuspendedTask.getActiveContext());
  }

  public ActiveContextBridge getActiveContext() {
    return jactiveContext;
  }

  @Override
  public void close() {
  }

  @Override
  public String getId() {
    return taskId;
  }

  @Override
  public byte[] get() {
    return jsuspendedTask.get();
  }
}
