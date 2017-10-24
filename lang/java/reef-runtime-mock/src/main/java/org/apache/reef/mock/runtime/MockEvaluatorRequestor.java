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
import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.mock.request.AllocateEvaluator;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.util.UUID;

/**
 * mock evaluator requestor.
 */
@Unstable
@Private
public final class MockEvaluatorRequestor implements EvaluatorRequestor {

  private final InjectionFuture<MockRuntimeDriver> mockRuntimeDriver;

  private final InjectionFuture<MockClock> clock;

  @Inject
  MockEvaluatorRequestor(
      final InjectionFuture<MockClock> clock,
      final InjectionFuture<MockRuntimeDriver> mockRuntimeDriver) {
    this.clock = clock;
    this.mockRuntimeDriver = mockRuntimeDriver;
  }

  @Override
  public void submit(final EvaluatorRequest req) {
    if (this.clock.get().isClosed()) {
      throw new IllegalStateException("clock closed");
    }
    final NodeDescriptor nodeDescriptor = new MockNodeDescriptor();
    final MockEvaluatorDescriptor evaluatorDescriptor = new MockEvaluatorDescriptor(nodeDescriptor);
    for (int i = 0; i < req.getNumber(); i++) {
      final MockAllocatedEvalautor mockEvaluator = new MockAllocatedEvalautor(
          this.mockRuntimeDriver.get(), UUID.randomUUID().toString(), evaluatorDescriptor);
      this.mockRuntimeDriver.get().add(new AllocateEvaluator(mockEvaluator));
    }
  }

  @Override
  public Builder newRequest() {
    if (this.clock.get().isClosed()) {
      throw new IllegalStateException("clock closed");
    }
    return new Builder();
  }


  /**
   * {@link EvaluatorRequest.Builder} extended with a new submit method.
   * {@link EvaluatorRequest}s are built using this builder.
   */
  private final class Builder extends EvaluatorRequest.Builder<Builder> {
    @Override
    public void submit() {
      MockEvaluatorRequestor.this.submit(this.build());
    }
  }
}
