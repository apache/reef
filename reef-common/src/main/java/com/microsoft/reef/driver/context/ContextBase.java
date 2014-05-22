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
package com.microsoft.reef.driver.context;

import com.microsoft.reef.annotations.Provided;
import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Public;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.io.naming.Identifiable;
import com.microsoft.reef.util.Optional;

/**
 * A common base interface for all Driver-side representations of Contexts.
 */
@Public
@DriverSide
@Provided
public interface ContextBase extends Identifiable {

  /**
   * @return the ID of the Context.
   */
  @Override
  String getId();

  /**
   * @return the identifier of the Evaluator this Context is instantiated on.
   */
  String getEvaluatorId();

  /**
   * @return the ID of the parent context, if there is one.
   */
  Optional<String> getParentId();

  /**
   * @return the descriptor of the Evaluator this Context is on.
   */
  EvaluatorDescriptor getEvaluatorDescriptor();
}
