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

package org.apache.reef.mock.request;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.io.Tuple;
import org.apache.reef.mock.AutoCompletable;
import org.apache.reef.mock.ProcessRequest;
import org.apache.reef.mock.runtime.MockActiveContext;
import org.apache.reef.mock.runtime.MockFailedContext;
import org.apache.reef.mock.runtime.MockRunningTask;
import org.apache.reef.util.Optional;

/**
 * create context and task process request.
 */
public class CreateContextAndTask implements
    ProcessRequestInternal<Tuple<MockActiveContext, MockRunningTask>, Tuple<MockFailedContext, FailedTask>>,
    AutoCompletable {

  private final MockActiveContext context;

  private final MockRunningTask task;

  private boolean autoComplete = true;

  public CreateContextAndTask(
      final MockActiveContext context,
      final MockRunningTask task) {
    this.context = context;
    this.task = task;
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
    return new CompleteTask(this.task);
  }
}
