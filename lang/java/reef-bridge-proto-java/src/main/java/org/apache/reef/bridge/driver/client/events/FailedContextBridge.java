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
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.exception.EvaluatorException;
import org.apache.reef.util.Optional;

/**
 * Failed context bridge.
 */
@Private
public final class FailedContextBridge implements FailedContext {

  private final String contextId;

  private final String evaluatorId;

  private final String message;

  private final EvaluatorDescriptor evaluatorDescriptor;

  private final Optional<ActiveContext> parentContext;

  private final Optional<Throwable> reason;

  public FailedContextBridge(
      final String contextId,
      final String evaluatorId,
      final String message,
      final EvaluatorDescriptor evaluatorDescriptor,
      final Optional<ActiveContext> parentContext,
      final Optional<Throwable> reason) {
    this.contextId = contextId;
    this.evaluatorId = evaluatorId;
    this.message = message;
    this.evaluatorDescriptor = evaluatorDescriptor;
    this.parentContext = parentContext;
    this.reason = reason;
  }

  @Override
  public Optional<ActiveContext> getParentContext() {
    return this.parentContext;
  }

  @Override
  public String getMessage() {
    return this.message;
  }

  @Override
  public Optional<String> getDescription() {
    return Optional.of(message);
  }

  @Override
  public Optional<Throwable> getReason() {
    return this.reason;
  }

  @Override
  public Optional<byte[]> getData() {
    return Optional.empty();
  }

  @Override
  public Throwable asError() {
    return new EvaluatorException(this.evaluatorId, this.message);
  }

  @Override
  public String getEvaluatorId() {
    return this.evaluatorId;
  }

  @Override
  public Optional<String> getParentId() {
    return this.parentContext.isPresent() ?
        Optional.of(this.parentContext.get().getId()) : Optional.<String>empty();
  }

  @Override
  public EvaluatorDescriptor getEvaluatorDescriptor() {
    return this.evaluatorDescriptor;
  }

  @Override
  public String getId() {
    return this.contextId;
  }
}
