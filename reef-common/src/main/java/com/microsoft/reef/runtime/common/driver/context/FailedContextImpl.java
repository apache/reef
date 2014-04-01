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
import com.microsoft.reef.driver.context.FailedContext;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.util.Optional;

/**
 * Driver-Side representation of a failed context.
 */
@Private
@DriverSide
public final class FailedContextImpl extends FailedContext {

  private final Optional<ActiveContext> parentContext;
  private final String evaluatorID;
  private final EvaluatorDescriptor evaluatorDescriptor;

  /**
   * @param cause               the exception thrown
   * @param contextId           the ID of the failed context
   * @param parentContext       the parent context that is now Active, if any
   * @param evaluatorID         the ID of the evaluator on which the context failed
   * @param evaluatorDescriptor the descriptor of the evaluator on which the context failed.
   */
  public FailedContextImpl(final Throwable cause,
                           final String contextId,
                           final Optional<ActiveContext> parentContext,
                           final String evaluatorID,
                           final EvaluatorDescriptor evaluatorDescriptor) {
    super(contextId, cause);
    this.parentContext = parentContext;
    this.evaluatorID = evaluatorID;
    this.evaluatorDescriptor = evaluatorDescriptor;
  }


  @Override
  public Optional<ActiveContext> getParentContext() {
    return this.parentContext;
  }

  @Override
  public String getEvaluatorId() {
    return this.evaluatorID;
  }

  @Override
  public Optional<String> getParentId() {
    if (this.getParentContext().isPresent()) {
      return Optional.of(this.getParentContext().get().getId());
    } else {
      return Optional.empty();
    }
  }

  @Override
  public EvaluatorDescriptor getEvaluatorDescriptor() {
    return this.evaluatorDescriptor;
  }

  @Override
  public String toString() {
    return "FailedContext{" +
        "contextID='" + this.getId() + "', " +
        "evaluatorID='" + evaluatorID + '\'' +
        '}';
  }
}
