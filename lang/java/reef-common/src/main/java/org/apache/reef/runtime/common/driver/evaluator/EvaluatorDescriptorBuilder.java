/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.reef.runtime.common.driver.evaluator;

import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorProcess;

/**
 * A builder for evaluator descriptors.
 */
public interface EvaluatorDescriptorBuilder extends org.apache.reef.util.Builder<EvaluatorDescriptor> {

  /**
   * Set the node descriptor for this evaluator.
   * @param nodeDescriptor for this evaluator
   * @return this
   */
  EvaluatorDescriptorBuilder setNodeDescriptor(final NodeDescriptor nodeDescriptor);

  /**
   * Amount of memory dedicated to this evaluator.
   * @param megaBytes of dedicated memory
   * @return this
   */
  EvaluatorDescriptorBuilder setMemory(final int megaBytes);

  /**
   * Set the number of cores.
   * @param numberOfCores dedicated for this evaluator
   * @return this
   */
  EvaluatorDescriptorBuilder setNumberOfCores(final int numberOfCores);

  /**
   * The process used to run this evaluator.
   * @param evaluatorProcess for this evaluator
   * @return this
   */
  EvaluatorDescriptorBuilder setEvaluatorProcess(final EvaluatorProcess evaluatorProcess);

  /**
   * The runtime name for this evaluator.
   * @param runtimeName for this evaluator
   * @return this
   */
  EvaluatorDescriptorBuilder setRuntimeName(final String runtimeName);
}
