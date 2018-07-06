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
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.exception.EvaluatorException;
import org.apache.reef.util.Optional;

import java.util.List;

/**
 * Failed Evaluator bridge.
 */
@Private
public final class FailedEvaluatorBridge implements FailedEvaluator {

  private final String id;

  private final EvaluatorException evaluatorException;

  private final List<FailedContext> failedContextList;

  private Optional<FailedTask> failedTask;

  public FailedEvaluatorBridge(
      final String id,
      final EvaluatorException evaluatorException,
      final List<FailedContext> failedContextList,
      final Optional<FailedTask> failedTask) {
    this.id = id;
    this.evaluatorException = evaluatorException;
    this.failedContextList = failedContextList;
    this.failedTask = failedTask;
  }

  @Override
  public EvaluatorException getEvaluatorException() {
    return this.evaluatorException;
  }

  @Override
  public List<FailedContext> getFailedContextList() {
    return this.failedContextList;
  }

  @Override
  public Optional<FailedTask> getFailedTask() {
    return this.failedTask;
  }

  @Override
  public String getId() {
    return this.id;
  }
}
