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

import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.io.Message;

public class CompletedTaskBridge extends NativeBridge implements Message {

  /**
   *  These fields are used by the C++ code. Please do not remove without testing.
   */
  private final CompletedTask jcompletedTask;
  private final String taskId;
  private final ActiveContextBridge jactiveContext;

  public CompletedTaskBridge(final CompletedTask completedTask, final ActiveContextBridgeFactory factory) {
    jcompletedTask = completedTask;
    taskId = completedTask.getId();
    jactiveContext = factory.getActiveContextBridge(completedTask.getActiveContext());
  }

  @Override
  public void close() {
  }

  @Override
  public byte[] get() {
    return jcompletedTask.get();
  }
}
