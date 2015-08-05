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
package org.apache.reef.driver.restart;

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * Records allocated evaluators for recovery on driver restart by using a DriverRuntimeRestartManager.
 */
public final class EvaluatorPreservingEvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
  private final DriverRestartManager driverRestartManager;

  @Inject
  private EvaluatorPreservingEvaluatorAllocatedHandler(final DriverRestartManager driverRestartManager) {
    this.driverRestartManager = driverRestartManager;
  }

  /**
   * Records the allocatedEvaluator ID with the DriverRuntimeRestartManager.
   * @param value the allocated evaluator event.
   */
  @Override
  public void onNext(final AllocatedEvaluator value) {
    this.driverRestartManager.recordAllocatedEvaluator(value.getId());
  }
}
