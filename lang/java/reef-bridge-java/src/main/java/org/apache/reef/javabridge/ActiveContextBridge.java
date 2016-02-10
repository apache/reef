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

import org.apache.commons.lang3.StringUtils;
import org.apache.reef.annotations.audience.Interop;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.io.naming.Identifiable;
import org.apache.reef.runtime.common.driver.context.EvaluatorContext;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Java-CLR bridge object for {@link org.apache.reef.driver.context.ActiveContext}.
 */
@Private
@Interop(
    CppFiles = { "Clr2JavaImpl.h", "ActiveContextClr2Java.cpp" },
    CsFiles = { "IActiveContextClr2Java.cs", "ActiveContext.cs" })
public final class ActiveContextBridge extends NativeBridge implements Identifiable {
  private static final Logger LOG = Logger.getLogger(ActiveContextBridge.class.getName());

  private final ActiveContext jactiveContext;
  private final String contextId;
  private final String evaluatorId;

  ActiveContextBridge(final ActiveContext activeContext) {
    this.jactiveContext = activeContext;
    this.contextId = activeContext.getId();
    this.evaluatorId = activeContext.getEvaluatorId();
  }

  /**
   * @return the context ID.
   */
  public String getContextId() {
    return contextId;
  }

  /**
   * @return the context ID of the parent.
   */
  public String getParentContextId() {
    return jactiveContext.getParentId().get();
  }

  /**
   * @return the Evaluator ID of the Evaluator on which the Context runs.
   */
  public String getEvaluatorId() {
    return evaluatorId;
  }

  public void submitTaskString(final String taskConfigurationString) {
    if (StringUtils.isEmpty(taskConfigurationString)) {
      throw new RuntimeException("empty taskConfigurationString provided.");
    }

    //when submit over the bridge, we would keep the task configuration as a serialized string
    //submitTask(String taskConfig) is not exposed in the interface. Therefore cast is necessary.
    ((EvaluatorContext)jactiveContext).submitTask(taskConfigurationString);
  }

  public void submitContextString(final String contextConfigurationString) {
    if (StringUtils.isEmpty(contextConfigurationString)) {
      throw new RuntimeException("empty contextConfigurationString provided.");
    }

    ((EvaluatorContext)jactiveContext).submitContext(contextConfigurationString);
  }

  public String getEvaluatorDescriptorString() {
    final String descriptorString = Utilities.getEvaluatorDescriptorString(jactiveContext.getEvaluatorDescriptor());
    LOG.log(Level.FINE, "active context - serialized evaluator descriptor: " + descriptorString);
    return descriptorString;
  }

  @Override
  public void close() {
    jactiveContext.close();
  }

  @Override
  public String getId() {
    return jactiveContext.getId();
  }
}
