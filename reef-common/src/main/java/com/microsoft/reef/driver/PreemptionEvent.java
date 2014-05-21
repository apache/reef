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
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;

/**
 * Represents Preemption requests issued by the underlying resource manager.
 * <p/>
 * REEF exposes two kinds of preemption requests: Strict ones merely inform the Driver about machines that are about to
 * be preempted. Flexible ones provide that list, but also expose the resource request that the underlying resource
 * manager wants to satisfy, thereby giving the Driver a chance to satisfy it in another way.
 * <p/>
 * NOTE: This currently not implemented. Consider it a preview of the API.
 */
@DriverSide
@Public
@Provided
@Unstable
public interface PreemptionEvent {

  /**
   * @return the Set of RunningEvaluators that the underlying resource manager is about to take away from the Driver.
   */
  // TODO: We need to have a set of things to present to the user as preempted. Probably a Set<String> with the Evaluator IDs.
  // public Set<RunningEvaluator> getToBePreemptedEvaluators();

}
