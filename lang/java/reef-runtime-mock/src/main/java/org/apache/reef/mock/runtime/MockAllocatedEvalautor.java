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

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorProcess;
import org.apache.reef.driver.task.TaskConfigurationOptions;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.mock.request.CloseEvaluator;
import org.apache.reef.mock.request.CreateContextAndTask;
import org.apache.reef.tang.Configuration;
import org.apache.reef.util.Optional;

import java.io.File;

/**
 * mock allocated evaluator.
 */
public final class MockAllocatedEvalautor implements AllocatedEvaluator {
  public static final String ROOT_CONTEXT_IDENTIFIER_PREFIX = "ROOT.CONTEXT.";

  private final MockRuntimeDriver mockRuntimeDriver;

  private final String identifier;

  private final EvaluatorDescriptor evaluatorDescriptor;

  private final MockActiveContext rootContext;

  private boolean closed = false;

  MockAllocatedEvalautor(
      final MockRuntimeDriver mockRuntimeDriver,
      final String identifier,
      final EvaluatorDescriptor evaluatorDescriptor) {
    this.mockRuntimeDriver = mockRuntimeDriver;
    this.identifier = identifier;
    this.evaluatorDescriptor = evaluatorDescriptor;
    this.rootContext = new MockActiveContext(
        mockRuntimeDriver,
        this,
        Optional.<MockActiveContext>empty(),
        ROOT_CONTEXT_IDENTIFIER_PREFIX + identifier);
  }

  public MockActiveContext getRootContext() {
    return this.rootContext;
  }

  @Override
  public void addFile(final File file) {
    // ignore
  }

  @Override
  public void addLibrary(final File file) {
    // ignore
  }

  @Override
  public EvaluatorDescriptor getEvaluatorDescriptor() {
    return this.evaluatorDescriptor;
  }

  @Override
  public void setProcess(final EvaluatorProcess process) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    if (!this.closed) {
      this.mockRuntimeDriver.add(new CloseEvaluator(this));
    } else {
      throw new IllegalStateException("evaluator already closed");
    }
  }

  @Override
  public void submitTask(final Configuration taskConfiguration) {
    this.rootContext.submitTask(taskConfiguration);
  }

  @Override
  public void submitContext(final Configuration contextConfiguration) {
    this.rootContext.submitContext(contextConfiguration);
  }

  @Override
  public void submitContextAndService(
      final Configuration contextConfiguration,
      final Configuration serviceConfiguration) {
    this.rootContext.submitContextAndService(contextConfiguration, serviceConfiguration);
  }

  @Override
  public void submitContextAndTask(
      final Configuration contextConfiguration,
      final Configuration taskConfiguration) {
    final String contextID = MockUtils.getValue(contextConfiguration, ContextIdentifier.class);
    final String taskID = MockUtils.getValue(taskConfiguration, TaskConfigurationOptions.Identifier.class);
    final MockActiveContext mockContext = new MockActiveContext(
        this.mockRuntimeDriver,
        this,
        Optional.of(this.rootContext),
        contextID);
    final MockRunningTask mockTask = new MockRunningTask(this.mockRuntimeDriver, taskID, mockContext);
    this.mockRuntimeDriver.add(
        new CreateContextAndTask(
            mockContext,
            mockTask,
            this.mockRuntimeDriver.getTaskReturnValueProvider()));
  }

  @Override
  public void submitContextAndServiceAndTask(
      final Configuration contextConfiguration,
      final Configuration serviceConfiguration,
      final Configuration taskConfiguration) {
    submitContextAndTask(contextConfiguration, taskConfiguration);
  }

  @Override
  public String getId() {
    return this.identifier;
  }
}
