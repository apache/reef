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

package org.apache.reef.mock.driver.request;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.mock.driver.ProcessRequest;
import org.apache.reef.mock.driver.runtime.MockAllocatedEvalautor;
import org.apache.reef.mock.driver.runtime.MockFailedEvaluator;

/**
 * Allocate Evaluator process request.
 */
@Unstable
@Private
public final class AllocateEvaluator implements
    ProcessRequestInternal<MockAllocatedEvalautor, FailedEvaluator> {

  private final MockAllocatedEvalautor evaluator;

  public AllocateEvaluator(final MockAllocatedEvalautor evaluator) {
    this.evaluator = evaluator;
  }

  @Override
  public Type getType() {
    return Type.ALLOCATE_EVALUATOR;
  }

  @Override
  public MockAllocatedEvalautor getSuccessEvent() {
    return this.evaluator;
  }

  @Override
  public FailedEvaluator getFailureEvent() {
    return new MockFailedEvaluator(evaluator.getId());
  }

  @Override
  public boolean doAutoComplete() {
    return false;
  }

  @Override
  public void setAutoComplete(final boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ProcessRequest getCompletionProcessRequest() {
    throw new UnsupportedOperationException();
  }
}
