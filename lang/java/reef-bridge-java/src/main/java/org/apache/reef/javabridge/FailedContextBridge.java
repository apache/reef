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
package org.apache.reef.javabridge;

import org.apache.reef.annotations.audience.Interop;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextBase;
import org.apache.reef.driver.context.FailedContext;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.util.Optional;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Java-CLR bridge object for {@link org.apache.reef.driver.context.FailedContext}.
 */
@Private
@Interop(
    CppFiles = { "Clr2JavaImpl.h", "FailedContextClr2Java.cpp" },
    CsFiles = { "IFailedContextClr2Java.cs", "FailedContext.cs" })
public final class FailedContextBridge extends NativeBridge implements ContextBase {

  private static final Logger LOG = Logger.getLogger(FailedContextBridge.class.getName());

  private final ActiveContextBridge parentContext;
  private final EvaluatorDescriptor evaluatorDescriptor;
  private final String evaluatorId;
  private final String contextId;
  private final String parentContextId;
  private final FailedContext jfailedContext;

  public FailedContextBridge(final FailedContext failedContext, final ActiveContextBridgeFactory factory) {
    jfailedContext = failedContext;
    evaluatorDescriptor = failedContext.getEvaluatorDescriptor();
    evaluatorId = failedContext.getEvaluatorId();
    contextId = failedContext.getId();
    if (failedContext.getParentContext().isPresent()) {
      final ActiveContext parent = failedContext.getParentContext().get();
      this.parentContextId = parent.getId();
      this.parentContext = factory.getActiveContextBridge(parent);
    } else {
      this.parentContextId = null;
      this.parentContext = null;
    }

  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public String getId() {
    return contextId;
  }

  @Override
  public String getEvaluatorId() {
    return evaluatorId;
  }

  @Override
  public Optional<String> getParentId() {
    return Optional.ofNullable(this.parentContextId);
  }

  @Override
  public EvaluatorDescriptor getEvaluatorDescriptor() {
    return evaluatorDescriptor;
  }

  /**
   * Used by InterOp code.
   */
  public String getParentIdString() {
    return this.parentContextId;
  }

  /**
   * @return the parent {@link ActiveContextBridge}.
   */
  public ActiveContextBridge getParentContext() {
    return parentContext;
  }

  public String getEvaluatorDescriptorString() {
    final String descriptorString = Utilities.getEvaluatorDescriptorString(evaluatorDescriptor);
    LOG.log(Level.INFO, "Failed Context - serialized evaluator descriptor: " + descriptorString);
    return descriptorString;
  }
}
