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
import org.apache.reef.io.naming.Identifiable;
import org.apache.reef.runtime.common.driver.evaluator.AllocatedEvaluatorImpl;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The AllocatedEvaluatorBridge object to bridge operations between REEF .NET and Java allocated evaluator operations.
 */
@Private
@Interop(
    CppFiles = { "Clr2JavaImpl.h", "AllocatedEvaluatorClr2Java.cpp" },
    CsFiles = { "IAllocatedEvaluatorClr2Java.cs", "AllocatedEvaluator.cs" })
public final class AllocatedEvaluatorBridge extends NativeBridge implements Identifiable {

  private static final Logger LOG = Logger.getLogger(AllocatedEvaluatorBridge.class.getName());

  private final AllocatedEvaluator jallocatedEvaluator;
  private final String evaluatorId;
  private final String nameServerInfo;

  /**
   * This constructor should only be called by the AllocatedEvaluatorBridgeFactory.
   */
  AllocatedEvaluatorBridge(final AllocatedEvaluator allocatedEvaluator,
                           final String serverInfo) {
    this.jallocatedEvaluator = allocatedEvaluator;
    this.evaluatorId = allocatedEvaluator.getId();
    this.nameServerInfo = serverInfo;
  }

  /**
   * Bridge function for REEF .NET to submit context and task configurations for the allocated evaluator.
   * @param contextConfigurationString the context configuration from .NET.
   * @param taskConfigurationString the task configuration from .NET.
   */
  public void submitContextAndTaskString(final String contextConfigurationString,
                                         final String taskConfigurationString) {
    if (contextConfigurationString.isEmpty()) {
      throw new RuntimeException("empty contextConfigurationString provided.");
    }
    if (taskConfigurationString.isEmpty()) {
      throw new RuntimeException("empty taskConfigurationString provided.");
    }
    //When submit over the bridge, we would keep the task configurations as a serialized strings.
    //submitContextAndTask(final String contextConfiguration,
    //final String taskConfiguration) is not exposed in the interface. Therefore cast is necessary.
    ((AllocatedEvaluatorImpl)jallocatedEvaluator)
        .submitContextAndTask(contextConfigurationString, taskConfigurationString);
  }

  /**
   * Bridge function for REEF .NET to submit context configuration for the allocated evaluator.
   * @param contextConfigurationString the context configuration from .NET.
   */
  public void submitContextString(final String contextConfigurationString) {
    if (contextConfigurationString.isEmpty()) {
      throw new RuntimeException("empty contextConfigurationString provided.");
    }

    //When submit over the bridge, we would keep the contextConfigurationString as serialized strings.
    //public void submitContext(final String contextConfiguration)
    // is not exposed in the interface. Therefore cast is necessary.
    ((AllocatedEvaluatorImpl)jallocatedEvaluator).submitContext(contextConfigurationString);
  }

  /**
   * Bridge function for REEF .NET to submit context and service configurations for the allocated evaluator.
   * @param contextConfigurationString the context configuration from .NET.
   * @param serviceConfigurationString the service configuration from .NET.
   */
  public void submitContextAndServiceString(final String contextConfigurationString,
                                            final String serviceConfigurationString) {
    if (contextConfigurationString.isEmpty()) {
      throw new RuntimeException("empty contextConfigurationString provided.");
    }
    if (serviceConfigurationString.isEmpty()) {
      throw new RuntimeException("empty serviceConfigurationString provided.");
    }

    //When submit over the bridge, we would keep the configurations as serialized strings.
    //public void submitContextAndService(final String contextConfiguration,
    //final String serviceConfiguration) is not exposed in the interface. Therefore cast is necessary.
    ((AllocatedEvaluatorImpl)jallocatedEvaluator)
        .submitContextAndService(contextConfigurationString, serviceConfigurationString);
  }

  /**
   * Bridge function for REEF .NET to submit context, service. and task configurations for the allocated evaluator.
   * @param contextConfigurationString the context configuration from .NET.
   * @param serviceConfigurationString the service configuration from .NET.
   * @param taskConfigurationString the task configuration from .NET.
   */
  public void submitContextAndServiceAndTaskString(
      final String contextConfigurationString,
      final String serviceConfigurationString,
      final String taskConfigurationString) {
    if (contextConfigurationString.isEmpty()) {
      throw new RuntimeException("empty contextConfigurationString provided.");
    }
    if (serviceConfigurationString.isEmpty()) {
      throw new RuntimeException("empty serviceConfigurationString provided.");
    }
    if (taskConfigurationString.isEmpty()) {
      throw new RuntimeException("empty taskConfigurationString provided.");
    }

    //When submit over the bridge, we would keep the task configuration as a serialized string.
    //submitContextAndServiceAndTask(final Configuration contextConfiguration, final Configuration serviceConfiguration,
    //final String taskConfiguration) is not exposed in the interface. Therefore cast is necessary.
    ((AllocatedEvaluatorImpl)jallocatedEvaluator).submitContextAndServiceAndTask(
        contextConfigurationString, serviceConfigurationString, taskConfigurationString);
  }

  /**
   * Gets the serialized evaluator descriptor from the Java allocated evaluator.
   * @return the serialized evaluator descriptor.
   */
  public String getEvaluatorDescriptorSring() {
    final String descriptorString =
        Utilities.getEvaluatorDescriptorString(jallocatedEvaluator.getEvaluatorDescriptor());
    LOG.log(Level.INFO, "allocated evaluator - serialized evaluator descriptor: " + descriptorString);
    return descriptorString;
  }

  /**
   * @return the nameServerInfo string.
   */
  public String getNameServerInfo() {
    return nameServerInfo;
  }

  /**
   * @return the evaluator id.
   */
  @Override
  public String getId() {
    return evaluatorId;
  }

  /**
   * Closes the Java AllocatedEvaluator.
   */
  @Override
  public void close() {
    jallocatedEvaluator.close();
  }
}
