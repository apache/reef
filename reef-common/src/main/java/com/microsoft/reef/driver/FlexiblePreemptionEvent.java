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
package com.microsoft.reef.driver;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.Unstable;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;

import java.util.Set;

/**
 * Represents a flexible preemption request: It contains:
 * <p/>
 * <ol>
 * <li>a set of EvaluatorRequests that the resource manager wants to have satisfied and also</li>
 * <li>a set of Evaluators it will choose to kill if the request isn't satisfied otherwise.</li>
 * </ol>
 * <p/>
 * NOTE: This currently not implemented. Consider it a preview of the API.
 */
@Private
@Provided
@Unstable
public interface FlexiblePreemptionEvent extends PreemptionEvent {

  /**
   * @return the set of EvaluatorRequests that the underlying resource manager seeks to satisfy.
   */
  public Set<EvaluatorRequest> getEvaluatorRequest();
}
