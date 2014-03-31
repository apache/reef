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
package com.microsoft.reef.runtime.common.driver.context;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ClosedContext;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.util.Optional;

/**
 * Driver side representation of a closed context.
 */
@DriverSide
@Private
public final class ClosedContextImpl implements ClosedContext {


  private final ActiveContext parentContext;
  private final String contextID;
  private final String evaluatorId;
  private final EvaluatorDescriptor evaluatorDescriptor;

  /**
   * @param parentContext       the parent context.
   * @param contextID           the id of the closed context
   * @param evaluatorId         the id of the evaluator on which the context was closed
   * @param evaluatorDescriptor the descriptor of the evaluator on which the context was closed.
   */
  public ClosedContextImpl(final ActiveContext parentContext,
                           final String contextID,
                           final String evaluatorId,
                           final EvaluatorDescriptor evaluatorDescriptor) {
    this.parentContext = parentContext;
    this.contextID = contextID;
    this.evaluatorId = evaluatorId;
    this.evaluatorDescriptor = evaluatorDescriptor;
  }

  @Override
  public ActiveContext getParentContext() {
    return parentContext;
  }

  @Override
  public String getId() {
    return this.contextID;
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

  @Override
  public String toString() {
    return "ClosedContext{" +
        "contextID='" + contextID + '\'' +
        '}';
  }
}
