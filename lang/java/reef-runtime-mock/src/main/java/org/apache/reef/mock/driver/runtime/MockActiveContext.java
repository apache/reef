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

package org.apache.reef.mock.driver.runtime;

import org.apache.reef.annotations.Unstable;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.mock.driver.request.CloseContext;
import org.apache.reef.mock.driver.request.CreateContext;
import org.apache.reef.mock.driver.request.CreateTask;
import org.apache.reef.mock.driver.request.SendMessageDriverToContext;
import org.apache.reef.tang.Configuration;
import org.apache.reef.util.Optional;

/**
 * mock active context.
 */
@Unstable
@Private
public final class MockActiveContext implements ActiveContext {

  private final MockRuntimeDriver mockRuntimeDriver;

  private final MockAllocatedEvalautor evaluator;

  private final Optional<MockActiveContext> parentContext;

  private final String contextID;

  MockActiveContext(
      final MockRuntimeDriver mockRuntimeDriver,
      final MockAllocatedEvalautor evalautor,
      final Optional<MockActiveContext> parentContext,
      final String contextID) {
    this.mockRuntimeDriver = mockRuntimeDriver;
    this.evaluator = evalautor;
    this.parentContext = parentContext;
    this.contextID = contextID;
  }

  @Override
  public int hashCode() {
    final String id = this.getEvaluatorId() + ":" + contextID;
    return id.hashCode();
  }

  public boolean equals(final Object that) {
    if (that instanceof MockActiveContext) {
      return this.getEvaluatorId().equals(((MockActiveContext)that).getEvaluatorId()) &&
          this.contextID.equals(((MockActiveContext)that).contextID);
    }
    return false;
  }

  public MockAllocatedEvalautor getEvaluator() {
    return this.evaluator;
  }

  public Optional<MockActiveContext> getParentContext() {
    return this.parentContext;
  }

  @Override
  public void close() {
    this.mockRuntimeDriver.add(new CloseContext(this));
  }

  @Override
  public void submitTask(final Configuration taskConf) {
    final String taskID = MockUtils.getValue(taskConf, TaskConfigurationOptions.Identifier.class);
    final MockRunningTask task = new MockRunningTask(this.mockRuntimeDriver, taskID, this);
    this.mockRuntimeDriver.add(new CreateTask(task, this.mockRuntimeDriver.getTaskReturnValueProvider()));
  }

  @Override
  public void submitContext(final Configuration contextConfiguration) {
    final String childContextID = MockUtils.getValue(contextConfiguration, ContextIdentifier.class);
    final MockActiveContext context = new MockActiveContext(
        this.mockRuntimeDriver,
        this.evaluator,
        Optional.of(this),
        childContextID);
    this.mockRuntimeDriver.add(new CreateContext(context));
  }

  @Override
  public void submitContextAndService(
      final Configuration contextConfiguration,
      final Configuration serviceConfiguration) {
    submitContext(contextConfiguration);
  }

  @Override
  public void sendMessage(final byte[] message) {
    this.mockRuntimeDriver.add(new SendMessageDriverToContext(this, message));
  }

  @Override
  public String getEvaluatorId() {
    return this.evaluator.getId();
  }

  @Override
  public Optional<String> getParentId() {
    return this.parentContext.isPresent() ?
        Optional.of(this.parentContext.get().getId()) :
        Optional.<String>empty();
  }

  @Override
  public EvaluatorDescriptor getEvaluatorDescriptor() {
    return this.evaluator.getEvaluatorDescriptor();
  }

  @Override
  public String getId() {
    return this.contextID;
  }
}
