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
package org.apache.reef.runtime.common.driver.evaluator;

import org.apache.commons.lang3.Validate;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.driver.catalog.ResourceCatalog;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorProcessFactory;
import org.apache.reef.runtime.common.driver.catalog.ResourceCatalogImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.*;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class that creates new EvaluatorManager instances from allocations.
 */
@Private
@DriverSide
public final class EvaluatorManagerFactory {
  private static final Logger LOG = Logger.getLogger(EvaluatorManagerFactory.class.getName());

  private final Injector injector;
  private final ResourceCatalog resourceCatalog;
  private final EvaluatorProcessFactory processFactory;
  private final EvaluatorDescriptorBuilderFactory evaluatorDescriptorBuilderFactory;

  @Inject
  EvaluatorManagerFactory(final Injector injector,
                          final ResourceCatalog resourceCatalog,
                          final EvaluatorProcessFactory processFactory,
                          final EvaluatorDescriptorBuilderFactory evaluatorDescriptorBuilderFactory) {
    this.injector = injector;
    this.resourceCatalog = resourceCatalog;
    this.processFactory = processFactory;
    this.evaluatorDescriptorBuilderFactory = evaluatorDescriptorBuilderFactory;
  }

  private EvaluatorManager getNewEvaluatorManagerInstanceForResource(
      final ResourceEvent resourceEvent) {
    NodeDescriptor nodeDescriptor = this.resourceCatalog.getNode(resourceEvent.getNodeId());

    if (nodeDescriptor == null) {
      final String nodeId = resourceEvent.getNodeId();
      LOG.log(Level.WARNING, "Node {0} is not in our catalog, adding it", nodeId);
      final String[] hostNameAndPort = nodeId.split(":");
      Validate.isTrue(hostNameAndPort.length == 2);
      final NodeDescriptorEvent nodeDescriptorEvent = NodeDescriptorEventImpl.newBuilder().setIdentifier(nodeId)
          .setHostName(hostNameAndPort[0]).setPort(Integer.parseInt(hostNameAndPort[1]))
          .setMemorySize(resourceEvent.getResourceMemory())
          .setRackName(resourceEvent.getRackName().get()).build();
      // downcasting not to change the API
      ((ResourceCatalogImpl) resourceCatalog).handle(nodeDescriptorEvent);
      nodeDescriptor = this.resourceCatalog.getNode(nodeId);
    }
    final EvaluatorDescriptor evaluatorDescriptor = evaluatorDescriptorBuilderFactory.newBuilder()
        .setNodeDescriptor(nodeDescriptor)
        .setMemory(resourceEvent.getResourceMemory())
        .setNumberOfCores(resourceEvent.getVirtualCores().get())
        .setEvaluatorProcess(processFactory.newEvaluatorProcess())
        .setRuntimeName(resourceEvent.getRuntimeName())
        .build();
    LOG.log(Level.FINEST, "Resource allocation: new evaluator id[{0}]", resourceEvent.getIdentifier());
    final EvaluatorManager evaluatorManager =
        getNewEvaluatorManagerInstance(resourceEvent.getIdentifier(), evaluatorDescriptor);

    return evaluatorManager;
  }

  /**
   * Helper method to create a new EvaluatorManager instance.
   *
   * @param id   identifier of the Evaluator
   * @param desc NodeDescriptor on which the Evaluator executes.
   * @return a new EvaluatorManager instance.
   */
  private EvaluatorManager getNewEvaluatorManagerInstance(final String id, final EvaluatorDescriptor desc) {
    LOG.log(Level.FINEST, "Creating Evaluator Manager for Evaluator ID {0}", id);
    final Injector child = this.injector.forkInjector();

    try {
      child.bindVolatileParameter(EvaluatorManager.EvaluatorIdentifier.class, id);
      child.bindVolatileParameter(EvaluatorManager.EvaluatorDescriptorName.class, desc);
    } catch (final BindException e) {
      throw new RuntimeException("Unable to bind evaluator identifier and name.", e);
    }

    final EvaluatorManager result;
    try {
      result = child.getInstance(EvaluatorManager.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("Unable to instantiate a new EvaluatorManager for Evaluator ID: " + id, e);
    }
    return result;
  }

  /**
   * Instantiates a new EvaluatorManager based on a resource allocation.
   * Fires the EvaluatorAllocatedEvent.
   *
   * @param resourceAllocationEvent
   * @return an EvaluatorManager for the newly allocated Evaluator.
   */
  public EvaluatorManager getNewEvaluatorManagerForNewEvaluator(
      final ResourceAllocationEvent resourceAllocationEvent) {
    final EvaluatorManager evaluatorManager = getNewEvaluatorManagerInstanceForResource(resourceAllocationEvent);
    evaluatorManager.fireEvaluatorAllocatedEvent();

    return evaluatorManager;
  }

  /**
   * Instantiates a new EvaluatorManager for a failed evaluator during driver restart.
   * Does not fire an EvaluatorAllocatedEvent.
   * @param resourceStatusEvent
   * @return an EvaluatorManager for the user to call fail on.
   */
  public EvaluatorManager getNewEvaluatorManagerForEvaluatorFailedDuringDriverRestart(
      final ResourceStatusEvent resourceStatusEvent) {
    return getNewEvaluatorManagerInstance(resourceStatusEvent.getIdentifier(),
        this.evaluatorDescriptorBuilderFactory.newBuilder()
            .setMemory(128)
            .setNumberOfCores(1)
            .setEvaluatorProcess(processFactory.newEvaluatorProcess())
            .setRuntimeName(resourceStatusEvent.getRuntimeName())
            .build());
  }

  /**
   * Instantiates a new EvaluatorManager based on a resource allocation from a recovered evaluator.
   *
   * @param resourceRecoverEvent
   * @return an EvaluatorManager for the newly allocated Evaluator.
   */
  public EvaluatorManager getNewEvaluatorManagerForRecoveredEvaluator(
      final ResourceRecoverEvent resourceRecoverEvent) {
    return getNewEvaluatorManagerInstanceForResource(resourceRecoverEvent);
  }
}
