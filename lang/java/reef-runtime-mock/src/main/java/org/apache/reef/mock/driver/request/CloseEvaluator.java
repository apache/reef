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
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.mock.driver.ProcessRequest;
import org.apache.reef.mock.driver.runtime.MockAllocatedEvaluator;
import org.apache.reef.mock.driver.runtime.MockFailedEvaluator;

/**
 * close evaluator request.
 */
@Unstable
@Private
public final class CloseEvaluator implements ProcessRequestInternal<CompletedEvaluator, FailedEvaluator> {

  private final MockAllocatedEvaluator evaluator;

  public CloseEvaluator(final MockAllocatedEvaluator evaluator) {
    this.evaluator = evaluator;
  }

  @Override
  public Type getType() {
    return Type.CLOSE_EVALUATOR;
  }

  @Override
  public CompletedEvaluator getSuccessEvent() {
    return new CompletedEvaluator() {
      @Override
      public String getId() {
        return evaluator.getId();
      }
    };
  }

  @Override
  public FailedEvaluator getFailureEvent() {
    // TODO[initialize remaining failed contstructer fields]
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
