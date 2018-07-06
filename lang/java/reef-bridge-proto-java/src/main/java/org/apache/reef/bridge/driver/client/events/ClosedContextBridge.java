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
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.util.Optional;

/**
 * Closed context bridge.
 */
@Private
public final class ClosedContextBridge implements ClosedContext {

  private final String contextId;

  private final String evaluatorId;

  private final ActiveContext parentContext;

  private final EvaluatorDescriptor evaluatorDescriptor;

  public ClosedContextBridge(
      final String contextId,
      final String evaluatorId,
      final ActiveContext parentContext,
      final EvaluatorDescriptor evaluatorDescriptor) {
    this.contextId = contextId;
    this.evaluatorId = evaluatorId;
    this.parentContext = parentContext;
    this.evaluatorDescriptor = evaluatorDescriptor;
  }

  @Override
  public ActiveContext getParentContext() {
    return this.parentContext;
  }

  @Override
  public String getId() {
    return this.contextId;
  }

  @Override
  public String getEvaluatorId() {
    return this.evaluatorId;
  }

  @Override
  public Optional<String> getParentId() {
    return Optional.of(this.parentContext.getId());
  }

  @Override
  public EvaluatorDescriptor getEvaluatorDescriptor() {
    return this.evaluatorDescriptor;
  }
}
