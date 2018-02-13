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
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.io.Tuple;
import org.apache.reef.mock.driver.AutoCompletable;
import org.apache.reef.mock.driver.MockTaskReturnValueProvider;
import org.apache.reef.mock.driver.ProcessRequest;
import org.apache.reef.mock.driver.runtime.MockActiveContext;
import org.apache.reef.mock.driver.runtime.MockFailedContext;
import org.apache.reef.mock.driver.runtime.MockRunningTask;
import org.apache.reef.util.Optional;

/**
 * create context and task process request.
 */
@Unstable
@Private
public final class CreateContextAndTask implements
    ProcessRequestInternal<Tuple<MockActiveContext, MockRunningTask>, Tuple<MockFailedContext, FailedTask>>,
    AutoCompletable {

  private final MockActiveContext context;

  private final MockRunningTask task;

  private final MockTaskReturnValueProvider taskReturnValueProvider;

  private boolean autoComplete = true;

  public CreateContextAndTask(
      final MockActiveContext context,
      final MockRunningTask task,
      final MockTaskReturnValueProvider taskReturnValueProvider) {
    this.context = context;
    this.task = task;
    this.taskReturnValueProvider = taskReturnValueProvider;
  }

  @Override
  public Type getType() {
    return Type.CREATE_CONTEXT_AND_TASK;
  }

  @Override
  public Tuple<MockActiveContext, MockRunningTask> getSuccessEvent() {
    return new Tuple<>(this.context, this.task);
  }

  @Override
  public Tuple<MockFailedContext, FailedTask> getFailureEvent() {
    return new Tuple<>(
        new MockFailedContext(this.context),
        new FailedTask(
            this.task.getId(),
            "mock",
            Optional.<String>empty(),
            Optional.<Throwable>empty(),
            Optional.<byte[]>empty(),
            Optional.of((ActiveContext)this.context)));
  }

  @Override
  public boolean doAutoComplete() {
    return this.autoComplete;
  }

  @Override
  public void setAutoComplete(final boolean value) {
    this.autoComplete = value;
  }

  @Override
  public ProcessRequest getCompletionProcessRequest() {
    return new CompleteTask(this.task, this.taskReturnValueProvider);
  }
}
