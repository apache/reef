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

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.tang.ClassHierarchy;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The AllocatedEvaluatorBridge object to bridge operations between REEF .NET and Java allocated evaluator operations.
 */
public final class AllocatedEvaluatorBridge extends NativeBridge {

  private static final Logger LOG = Logger.getLogger(AllocatedEvaluatorBridge.class.getName());

  private final AllocatedEvaluator jallocatedEvaluator;
  private final AvroConfigurationSerializer serializer;
  private final ClassHierarchy clrClassHierarchy;
  private final String evaluatorId;
  private final String nameServerInfo;

  /**
   * This constructor should only be called by the AllocatedEvaluatorBridgeFactory.
   */
  AllocatedEvaluatorBridge(final AllocatedEvaluator allocatedEvaluator,
                           final ClassHierarchy clrClassHierarchy,
                           final String serverInfo,
                           final AvroConfigurationSerializer serializer) {
    this.jallocatedEvaluator = allocatedEvaluator;
    this.serializer = serializer;
    this.clrClassHierarchy = clrClassHierarchy;
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
    final Configuration contextConfiguration;
    final Configuration taskConfiguration;
    try {
      contextConfiguration = serializer.fromString(contextConfigurationString, clrClassHierarchy);
      taskConfiguration = serializer.fromString(taskConfigurationString, clrClassHierarchy);
    } catch (final Exception e) {
      final String message = "Unable to de-serialize CLR context or task configurations using class hierarchy.";
      LOG.log(Level.SEVERE, message, e);
      throw new RuntimeException(message, e);
    }
    jallocatedEvaluator.submitContextAndTask(contextConfiguration, taskConfiguration);
  }

  /**
   * Bridge function for REEF .NET to submit context configuration for the allocated evaluator.
   * @param contextConfigurationString the context configuration from .NET.
   */
  public void submitContextString(final String contextConfigurationString) {
    if (contextConfigurationString.isEmpty()) {
      throw new RuntimeException("empty contextConfigurationString provided.");
    }
    final Configuration contextConfiguration;
    try {
      contextConfiguration = serializer.fromString(contextConfigurationString, clrClassHierarchy);
    } catch (final Exception e) {
      final String message = "Unable to de-serialize CLR context configurations using class hierarchy.";
      LOG.log(Level.SEVERE, message, e);
      throw new RuntimeException(message, e);
    }
    jallocatedEvaluator.submitContext(contextConfiguration);
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

    final Configuration contextConfiguration;
    final Configuration servicetConfiguration;
    try {
      contextConfiguration = serializer.fromString(contextConfigurationString, clrClassHierarchy);
      servicetConfiguration = serializer.fromString(serviceConfigurationString, clrClassHierarchy);
    } catch (final Exception e) {
      final String message = "Unable to de-serialize CLR context or service  configurations using class hierarchy.";
      LOG.log(Level.SEVERE, message, e);
      throw new RuntimeException(message, e);
    }
    jallocatedEvaluator.submitContextAndService(contextConfiguration, servicetConfiguration);
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
    final Configuration contextConfiguration;
    final Configuration servicetConfiguration;
    final Configuration taskConfiguration;
    try {
      contextConfiguration = serializer.fromString(contextConfigurationString, clrClassHierarchy);
      servicetConfiguration = serializer.fromString(serviceConfigurationString, clrClassHierarchy);
      taskConfiguration = serializer.fromString(taskConfigurationString, clrClassHierarchy);
    } catch (final Exception e) {
      final String message =
          "Unable to de-serialize CLR context or service or task configurations using class hierarchy.";
      LOG.log(Level.SEVERE, message, e);
      throw new RuntimeException(message, e);
    }
    jallocatedEvaluator.submitContextAndServiceAndTask(contextConfiguration, servicetConfiguration, taskConfiguration);
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
   * Closes the Java AllocatedEvaluator.
   */
  @Override
  public void close() {
    jallocatedEvaluator.close();
  }
}
