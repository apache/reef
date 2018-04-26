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

package org.apache.reef.mock.driver.runtime;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.util.Optional;

/**
 * mock failed context.
 */
@Unstable
@Private
public final class MockFailedContext implements FailedContext {

  private final MockActiveContext context;

  public MockFailedContext(final MockActiveContext context) {
    this.context = context;
  }

  @Override
  public Optional<ActiveContext> getParentContext() {
    return this.context.getParentContext().isPresent() ?
        Optional.of((ActiveContext)this.context.getParentContext().get()) :
        Optional.<ActiveContext>empty();
  }

  @Override
  public String getMessage() {
    return "mock";
  }

  @Override
  public Optional<String> getDescription() {
    return Optional.empty();
  }

  @Override
  public Optional<Throwable> getReason() {
    return Optional.empty();
  }

  @Override
  public Optional<byte[]> getData() {
    return Optional.empty();
  }

  @Override
  public Throwable asError() {
    return new Exception("mock");
  }

  @Override
  public String getEvaluatorId() {
    return this.context.getEvaluatorId();
  }

  @Override
  public Optional<String> getParentId() {
    return this.context.getParentId();
  }

  @Override
  public EvaluatorDescriptor getEvaluatorDescriptor() {
    return this.context.getEvaluatorDescriptor();
  }

  @Override
  public String getId() {
    return this.context.getId();
  }
}
