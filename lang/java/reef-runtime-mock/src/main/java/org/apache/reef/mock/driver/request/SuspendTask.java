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

package org.apache.reef.mock.driver.request;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.SuspendedTask;
import org.apache.reef.mock.driver.ProcessRequest;
import org.apache.reef.mock.driver.runtime.MockRunningTask;
import org.apache.reef.mock.driver.runtime.MockSuspendedTask;
import org.apache.reef.util.Optional;

/**
 * suspend task process request.
 */
@Unstable
@Private
public final class SuspendTask implements ProcessRequestInternal<SuspendedTask, FailedTask> {

  private final MockRunningTask task;

  private final Optional<byte[]> message;

  public SuspendTask(final MockRunningTask task, final Optional<byte[]> message) {
    this.task = task;
    this.message = message;
  }

  public MockRunningTask getTask() {
    return task;
  }

  @Override
  public Type getType() {
    return Type.SUSPEND_TASK;
  }

  public Optional<byte[]> getMessage() {
    return message;
  }

  @Override
  public MockSuspendedTask getSuccessEvent() {
    return new MockSuspendedTask(this.task);
  }

  @Override
  public FailedTask getFailureEvent() {
    return new FailedTask(
        this.task.getId(),
        "mock",
        Optional.<String>empty(),
        Optional.<Throwable>empty(),
        Optional.<byte[]>empty(),
        Optional.of(this.task.getActiveContext()));
  }

  @Override
  public boolean doAutoComplete() {
    return false;
  }

  @Override
  public void setAutoComplete(final boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ProcessRequest getCompletionProcessRequest() {
    throw new UnsupportedOperationException();
  }
}
