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

package org.apache.reef.bridge.driver.client.events;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.bridge.driver.client.IDriverServiceClient;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.runtime.common.driver.task.TaskRepresenter;
import org.apache.reef.util.Optional;

/**
 * Running task bridge.
 */
@Private
public final class RunningTaskBridge implements RunningTask {

  private final IDriverServiceClient driverServiceClient;

  private final String taskId;

  private final ActiveContext context;


  public RunningTaskBridge(
      final IDriverServiceClient driverServiceClient,
      final String taskId,
      final ActiveContext context) {
    this.driverServiceClient = driverServiceClient;
    this.taskId = taskId;
    this.context = context;
  }

  @Override
  public ActiveContext getActiveContext() {
    return this.context;
  }

  @Override
  public void send(final byte[] message) {
    this.driverServiceClient.onTaskMessage(this.taskId, message);
  }

  @Override
  public void suspend(final byte[] message) {
    this.driverServiceClient.onSuspendTask(this.taskId, Optional.of(message));
  }

  @Override
  public void suspend() {
    this.driverServiceClient.onSuspendTask(this.taskId, Optional.<byte[]>empty());
  }

  @Override
  public void close(final byte[] message) {
    this.driverServiceClient.onTaskClose(this.taskId, Optional.of(message));
  }

  @Override
  public void close() {
    this.driverServiceClient.onTaskClose(this.taskId, Optional.<byte[]>empty());
  }

  @Override
  public TaskRepresenter getTaskRepresenter() {
    throw new UnsupportedOperationException("Not a public API");
  }

  @Override
  public String getId() {
    return this.taskId;
  }
}
