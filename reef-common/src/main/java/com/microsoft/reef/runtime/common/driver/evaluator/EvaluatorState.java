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
package com.microsoft.reef.runtime.common.driver.evaluator;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;

/**
 * Various states that the EvaluatorManager could be in. The EvaluatorManager is
 * created when a resource has been allocated by the ResourceManager.
 */
@DriverSide
@Private
enum EvaluatorState {
  ALLOCATED,  // initial state
  SUBMITTED,  // client called AllocatedEvaluator.submitTask() and we're waiting for first contact
  RUNNING,    // first contact received, all communication channels established, Evaluator sent to client.
  // TODO: Add CLOSING state
  DONE,       // clean shutdown
  FAILED,     // some failure occurred.
  KILLED      // unclean shutdown
}
