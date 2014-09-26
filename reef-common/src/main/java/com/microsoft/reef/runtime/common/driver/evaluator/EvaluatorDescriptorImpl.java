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
import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.driver.evaluator.EvaluatorType;

/**
 * A simple all-data implementation of EvaluatorDescriptor
 */
@Private
@DriverSide
final class EvaluatorDescriptorImpl implements EvaluatorDescriptor {

  private final NodeDescriptor nodeDescriptor;
  private EvaluatorType type;
  private final int megaBytes;
  private final int numberOfCores;

  public EvaluatorDescriptorImpl(final NodeDescriptor nodeDescriptor,
                                 final EvaluatorType type,
                                 final int megaBytes,
                                 final int numberOfCores) {
    this.nodeDescriptor = nodeDescriptor;
    this.type = type;
    this.megaBytes = megaBytes;
    this.numberOfCores = numberOfCores;
  }

  @Override
  public NodeDescriptor getNodeDescriptor() {
    return this.nodeDescriptor;
  }

  @Override
  public synchronized EvaluatorType getType() {
    return this.type;
  }

  @Override
  public int getMemory() {
    return this.megaBytes;
  }

  @Override
  public int getNumberOfCores() {
    return this.numberOfCores;
  }

  public synchronized void setType(final EvaluatorType type) {
    if (this.getType() != EvaluatorType.UNDECIDED) {
      throw new RuntimeException("Unable to change state of an Evaluator of Type: " + this.getType());
    }
    this.type = type;
  }
}
