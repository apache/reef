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

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.driver.catalog.ResourceCatalog;
import org.apache.reef.driver.evaluator.EvaluatorProcessFactory;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceAllocationEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEvent;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Helper class that creates new EvaluatorManager instances from alloations.
 */
@Private
@DriverSide
public final class EvaluatorManagerFactory {
  private static final Logger LOG = Logger.getLogger(EvaluatorManagerFactory.class.getName());

  private final Injector injector;
  private final ResourceCatalog resourceCatalog;
  private final EvaluatorProcessFactory processFactory;

  @Inject
  EvaluatorManagerFactory(final Injector injector,
                          final ResourceCatalog resourceCatalog,
                          final EvaluatorProcessFactory processFactory) {
    this.injector = injector;
    this.resourceCatalog = resourceCatalog;
    this.processFactory = processFactory;
  }

  /**
   * Helper method to create a new EvaluatorManager instance.
   *
   * @param id   identifier of the Evaluator
   * @param desc NodeDescriptor on which the Evaluator executes.
   * @return a new EvaluatorManager instance.
   */
  private EvaluatorManager getNewEvaluatorManagerInstance(final String id, final EvaluatorDescriptorImpl desc) {
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
   *
   * @param resourceAllocationEvent
   * @return
   */
  public EvaluatorManager getNewEvaluatorManager(final ResourceAllocationEvent resourceAllocationEvent) {
    final NodeDescriptor nodeDescriptor = this.resourceCatalog.getNode(resourceAllocationEvent.getNodeId());

    if (nodeDescriptor == null) {
      throw new RuntimeException("Unknown resource: " + resourceAllocationEvent.getNodeId());
    }
    final EvaluatorDescriptorImpl evaluatorDescriptor = new EvaluatorDescriptorImpl(nodeDescriptor,
        resourceAllocationEvent.getResourceMemory(), resourceAllocationEvent.getVirtualCores().get(),
        processFactory.newEvaluatorProcess());

    LOG.log(Level.FINEST, "Resource allocation: new evaluator id[{0}]", resourceAllocationEvent.getIdentifier());
    return this.getNewEvaluatorManagerInstance(resourceAllocationEvent.getIdentifier(), evaluatorDescriptor);
  }

  public EvaluatorManager createForEvaluatorFailedDuringDriverRestart(final ResourceStatusEvent resourceStatusEvent) {
    if (!resourceStatusEvent.getIsFromPreviousDriver().get()) {
      throw new RuntimeException("Invalid resourceStatusEvent, must be status for resource from previous Driver.");
    }
    return getNewEvaluatorManagerInstance(resourceStatusEvent.getIdentifier(),
        new EvaluatorDescriptorImpl(null, 128, 1, processFactory.newEvaluatorProcess()));
  }
}
