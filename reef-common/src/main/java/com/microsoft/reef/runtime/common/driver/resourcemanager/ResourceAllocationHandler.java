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
package com.microsoft.reef.runtime.common.driver.resourcemanager;

import com.microsoft.reef.annotations.audience.DriverSide;
import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.runtime.common.driver.evaluator.EvaluatorManagerFactory;
import com.microsoft.reef.runtime.common.driver.evaluator.Evaluators;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;

/**
 * Handles new resource allocations by adding a new EvaluatorManager.
 */
@Private
@DriverSide
public final class ResourceAllocationHandler
    implements EventHandler<DriverRuntimeProtocol.ResourceAllocationProto> {

  /**
   * Helper class to make new EvaluatorManager instances,
   * given a Node they have been allocated on.
   */
  private final EvaluatorManagerFactory evaluatorManagerFactory;

  /**
   * The Evaluators known to the Driver.
   */
  private final Evaluators evaluators;

  @Inject
  ResourceAllocationHandler(
      final EvaluatorManagerFactory evaluatorManagerFactory, final Evaluators evaluators) {
    this.evaluatorManagerFactory = evaluatorManagerFactory;
    this.evaluators = evaluators;
  }

  @Override
  public void onNext(final DriverRuntimeProtocol.ResourceAllocationProto value) {
    // FIXME: Using this put() method is a temporary fix for the race condition
    // described in issues #828 and #839. Use Evaluators.put(EvaluatorManager) instead
    // when the bug is fixed.
    this.evaluators.put(this.evaluatorManagerFactory, value);
  }
}
