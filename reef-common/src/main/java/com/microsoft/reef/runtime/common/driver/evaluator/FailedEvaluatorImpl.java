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
package com.microsoft.reef.runtime.common.driver.evaluator;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.FailedEvaluator;
import com.microsoft.reef.driver.task.FailedTask;
import com.microsoft.reef.exception.EvaluatorException;
import com.microsoft.reef.util.Optional;

import java.util.List;

@DriverSide
@Private
final class FailedEvaluatorImpl implements FailedEvaluator {

  final String id;
  private final EvaluatorException ex;
  private final List<FailedContext> ctx;
  private final Optional<FailedTask> task;

  public FailedEvaluatorImpl(final EvaluatorException ex, final List<FailedContext> ctx, final Optional<FailedTask> task, final String id) {
    this.ex = ex;
    this.ctx = ctx;
    this.task = task;
    this.id = id;
  }

  @Override
  public EvaluatorException getEvaluatorException() {
    return this.ex;
  }

  @Override
  public List<FailedContext> getFailedContextList() {
    return this.ctx;
  }

  @Override
  public Optional<FailedTask> getFailedTask() {
    return this.task;
  }

  @Override
  public String getId() {
    return this.id;
  }

  @Override
  public String toString() {
    return "FailedEvaluator{" +
        "id='" + id + '\'' +
        '}';
  }
}
