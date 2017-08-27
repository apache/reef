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
package org.apache.reef.driver.evaluator;

import org.apache.reef.driver.catalog.NodeDescriptor;

import java.util.Map;

/**
 * Metadata about an Evaluator.
 */
public interface EvaluatorDescriptor {

  /**
   * @return the NodeDescriptor of the node where this Evaluator is running.
   */
  NodeDescriptor getNodeDescriptor();

  /**
   * @return the process to be run on the Evaluator.
   */
  EvaluatorProcess getProcess();

  /**
   * @return the amount of memory allocated to this Evaluator.
   */
  int getMemory();

  /**
   * @return the number of virtual core allocated to this Evaluator.
   */
  int getNumberOfCores();

  /**
   * @return name of the runtime that was used to allocate this Evaluator
   */
  String getRuntimeName();

  /**
   * @return node labels on this Evaluator
   */
  Map<String, String> getNodeLabels();
}
