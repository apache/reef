/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.reef.mock.driver.runtime;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.mock.driver.request.CloseTask;
import org.apache.reef.mock.driver.request.SendMessageDriverToTask;
import org.apache.reef.mock.driver.request.SuspendTask;
import org.apache.reef.runtime.common.driver.task.TaskRepresenter;
import org.apache.reef.util.Optional;

/**
 * mock running task.
 */
@Unstable
@Private
public final class MockRunningTask implements RunningTask {

  private final MockRuntimeDriver mockRuntimeDriver;

  private final String taskID;

  private final ActiveContext context;

  MockRunningTask(
      final MockRuntimeDriver mockRuntimeDriver,
      final String taskID,
      final ActiveContext context) {
    this.mockRuntimeDriver = mockRuntimeDriver;
    this.taskID = taskID;
    this.context = context;
  }

  public String evaluatorID() {
    return this.context.getEvaluatorId();
  }

  @Override
  public ActiveContext getActiveContext() {
    return this.context;
  }

  @Override
  public void send(final byte[] message) {
    this.mockRuntimeDriver.add(new SendMessageDriverToTask(this, message));
  }

  @Override
  public void suspend(final byte[] message) {
    this.mockRuntimeDriver.add(new SuspendTask(this, Optional.of(message)));
  }

  @Override
  public void suspend() {
    this.mockRuntimeDriver.add(new SuspendTask(this, Optional.<byte[]>empty()));
  }

  @Override
  public void close(final byte[] message) {
    this.mockRuntimeDriver.add(new CloseTask(this, this.mockRuntimeDriver.getTaskReturnValueProvider()));
  }

  @Override
  public void close() {
    this.mockRuntimeDriver.add(new CloseTask(this, this.mockRuntimeDriver.getTaskReturnValueProvider()));
  }

  @Override
  public TaskRepresenter getTaskRepresenter() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getId() {
    return this.taskID;
  }
}
