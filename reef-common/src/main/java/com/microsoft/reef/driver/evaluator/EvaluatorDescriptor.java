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
package com.microsoft.reef.driver.evaluator;

import com.microsoft.reef.driver.catalog.NodeDescriptor;

/**
 * Metadata about an Evaluator.
 */
public interface EvaluatorDescriptor {

  /**
   * @return the NodeDescriptor of the node where this Evaluator is running.
   */
  public NodeDescriptor getNodeDescriptor();

  /**
   * @return the type of Evaluator.
   */
  public EvaluatorType getType();

  /**
   * @return the amount of memory allocated to this Evaluator.
   */
  public int getMemory();

  /**
   * @return the number of virtual core allocated to this Evaluator.
   */
  public int getNumberOfCores();
}
