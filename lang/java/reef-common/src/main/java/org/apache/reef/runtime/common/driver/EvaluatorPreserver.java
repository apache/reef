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
package org.apache.reef.runtime.common.driver;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.annotations.audience.RuntimeAuthor;

import java.util.Set;

/**
 * A interface to preserve evaluators across driver restarts.
 */
@DriverSide
@Private
@RuntimeAuthor
@Unstable
public interface EvaluatorPreserver {
  /**
   * Called on driver restart when evaluators are to be recovered.
   */
  Set<String> recoverEvaluators();

  /**
   * Called when an evaluator is to be preserved.
   */
  void recordAllocatedEvaluator(String id);

  /**
   * Called when an evaluator is to be removed.
   * @param id
   */
  void recordRemovedEvaluator(String id);
}
