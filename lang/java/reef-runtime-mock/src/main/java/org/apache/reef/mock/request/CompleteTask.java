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

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.mock.MockTaskReturnValueProvider;
import org.apache.reef.mock.ProcessRequest;
import org.apache.reef.mock.runtime.MockCompletedTask;
import org.apache.reef.mock.runtime.MockRunningTask;

/**
 * close task process request.
 */
@Unstable
@Private
public final class CompleteTask implements ProcessRequestInternal<CompletedTask, FailedTask> {

  private final MockRunningTask task;

  private final MockTaskReturnValueProvider returnValueProvider;

  public CompleteTask(
      final MockRunningTask task,
      final MockTaskReturnValueProvider returnValueProvider) {
    this.task = task;
    this.returnValueProvider = returnValueProvider;
  }

  public MockRunningTask getTask() {
    return task;
  }

  @Override
  public Type getType() {
    return Type.COMPLETE_TASK;
  }

  @Override
  public CompletedTask getSuccessEvent() {
    return new MockCompletedTask(this.task, this.returnValueProvider.getReturnValue(task));
  }

  @Override
  public FailedTask getFailureEvent() {
    return null;
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
