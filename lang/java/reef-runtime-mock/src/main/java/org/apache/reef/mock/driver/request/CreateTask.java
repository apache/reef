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
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.mock.driver.AutoCompletable;
import org.apache.reef.mock.driver.MockTaskReturnValueProvider;
import org.apache.reef.mock.driver.ProcessRequest;
import org.apache.reef.mock.driver.runtime.MockRunningTask;
import org.apache.reef.util.Optional;

/**
 * create task process request.
 */
@Unstable
@Private
public final class CreateTask implements
    ProcessRequestInternal<RunningTask, FailedTask>,
    AutoCompletable {

  private final MockRunningTask task;

  private final MockTaskReturnValueProvider returnValueProvider;

  private boolean autoComplete = true;

  public CreateTask(
      final MockRunningTask task,
      final MockTaskReturnValueProvider returnValueProvider) {
    this.task = task;
    this.returnValueProvider = returnValueProvider;
  }

  @Override
  public Type getType() {
    return Type.CREATE_TASK;
  }

  @Override
  public boolean doAutoComplete() {
    return this.autoComplete;
  }

  @Override
  public ProcessRequest getCompletionProcessRequest() {
    return new CompleteTask(this.task, this.returnValueProvider);
  }

  @Override
  public void setAutoComplete(final boolean value) {
    this.autoComplete = value;
  }

  @Override
  public MockRunningTask getSuccessEvent() {
    return this.task;
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
}
