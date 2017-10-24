/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.reef.mock.runtime;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorProcess;

/**
 * mock evaluator descriptor.
 */
@Unstable
@Private
public final class MockEvaluatorDescriptor implements EvaluatorDescriptor {
  private final NodeDescriptor nodeDescriptor;

  MockEvaluatorDescriptor(final NodeDescriptor nodeDescriptor) {
    this.nodeDescriptor = nodeDescriptor;
  }

  @Override
  public NodeDescriptor getNodeDescriptor() {
    return this.nodeDescriptor;
  }

  @Override
  public EvaluatorProcess getProcess() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMemory() {
    return 0;
  }

  @Override
  public int getNumberOfCores() {
    return 1;
  }

  @Override
  public String getRuntimeName() {
    return "mock";
  }
}
