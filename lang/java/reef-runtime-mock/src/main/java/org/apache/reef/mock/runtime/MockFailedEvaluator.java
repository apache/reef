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
package org.apache.reef.mock.runtime;

import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.exception.EvaluatorException;
import org.apache.reef.util.Optional;

import java.util.ArrayList;
import java.util.List;

/**
 * mock failed evaluator.
 */
public class MockFailedEvaluator implements FailedEvaluator {

  private final String evaluatorID;

  private final List<FailedContext> failedContextList;

  private final Optional<FailedTask> failedTask;

  public MockFailedEvaluator(
      final String evaluatorID,
      final List<FailedContext> failedContextList,
      final Optional<FailedTask> failedTask) {
    this.evaluatorID = evaluatorID;
    this.failedContextList = failedContextList;
    this.failedTask = failedTask;
  }

  public MockFailedEvaluator(final String evaluatorID) {
    this.evaluatorID = evaluatorID;
    this.failedContextList = new ArrayList<>();
    this.failedTask = Optional.empty();
  }

  @Override
  public EvaluatorException getEvaluatorException() {
    return null;
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
    return this.evaluatorID;
  }
}
