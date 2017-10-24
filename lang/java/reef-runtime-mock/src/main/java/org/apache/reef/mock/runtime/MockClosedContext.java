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

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ClosedContext;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.util.Optional;

/**
 * mock closed context.
 */
@Unstable
@Private
public final class MockClosedContext implements ClosedContext {

  private final MockActiveContext mockActiveContext;

  public MockClosedContext(final MockActiveContext activeContext) {
    this.mockActiveContext = activeContext;
  }

  public MockActiveContext getMockActiveContext() {
    return this.mockActiveContext;
  }

  @Override
  public ActiveContext getParentContext() {
    return this.mockActiveContext.getParentContext().isPresent() ?
      this.mockActiveContext.getParentContext().get() : null;
  }

  @Override
  public String getId() {
    return this.mockActiveContext.getId();
  }

  @Override
  public String getEvaluatorId() {
    return this.mockActiveContext.getEvaluatorId();
  }

  @Override
  public Optional<String> getParentId() {
    return this.mockActiveContext.getParentId();
  }

  @Override
  public EvaluatorDescriptor getEvaluatorDescriptor() {
    return this.mockActiveContext.getEvaluatorDescriptor();
  }
}
