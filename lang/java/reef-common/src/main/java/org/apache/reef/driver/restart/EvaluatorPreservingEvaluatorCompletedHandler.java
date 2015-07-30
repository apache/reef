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

import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * Removes an evaluator from the evaluators to restart after it has completed.
 */
public final class EvaluatorPreservingEvaluatorCompletedHandler implements EventHandler<CompletedEvaluator> {
  private final DriverRestartManager driverRestartManager;

  @Inject
  private EvaluatorPreservingEvaluatorCompletedHandler(final DriverRestartManager driverRestartManager) {
    this.driverRestartManager = driverRestartManager;
  }

  /**
   * Removes the completed evaluator from the list of evaluators to recover
   * once it is completed.
   * @param value The completed evaluator event.
   */
  @Override
  public void onNext(final CompletedEvaluator value) {
    this.driverRestartManager.recordRemovedEvaluator(value.getId());
  }
}
