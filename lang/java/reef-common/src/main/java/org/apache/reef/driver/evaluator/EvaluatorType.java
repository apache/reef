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

/**
 * Enumeration of all Evaluator types supported by REEF.
 */
public enum EvaluatorType {
  /**
   * Indicates an Evaluator that runs on the JVM.
   */
  JVM,
  /**
   * Indicates an Evaluator that runs on the CLR.
   */
  CLR,
  /**
   * Indicates an Evaluator that runs in DOTNET.
   */
  DOTNET,
  /**
   * Indicates an Evaluator whose type hasn't been decided yet. This is common e.g. between Evaluator allocation
   * and launch.
   */
  UNDECIDED
}
