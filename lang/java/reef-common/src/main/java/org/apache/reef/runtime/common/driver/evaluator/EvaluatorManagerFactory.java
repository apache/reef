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

import org.apache.commons.lang.Validate;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.catalog.NodeDescriptor;
import org.apache.reef.driver.catalog.ResourceCatalog;
import org.apache.reef.driver.evaluator.EvaluatorProcessFactory;
import org.apache.reef.runtime.common.driver.catalog.ResourceCatalogImpl;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEventImpl;
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
   * Fires the EvaluatorAllocatedEvent.
   *
   * @param resourceAllocationEvent
   * @return an EvaluatorManager for the newly allocated Evaluator.
   */
  public EvaluatorManager getNewEvaluatorManagerForNewlyAllocatedEvaluator(
      final ResourceAllocationEvent resourceAllocationEvent) {
    NodeDescriptor nodeDescriptor = this.resourceCatalog.getNode(resourceAllocationEvent.getNodeId());

    if (nodeDescriptor == null) {
      LOG.log(Level.WARNING, "Node descriptor not found for node {0}", resourceAllocationEvent.getNodeId());

      // HOT FIX for YARN with FEDERATION
      // See JIRA REEF-568
      // Should be removed once the YARN-2915 is fixed
      if (resourceAllocationEvent.getRackName().isPresent()) {
        final String federationAsStr = resourceAllocationEvent.getRackName().get();
        final boolean federation = Boolean.valueOf(federationAsStr);
        // if a true value came here, means that we are using federation
        if (federation) {
          LOG.log(Level.WARNING, "Adding node {0}, hack to make it work with Federation",
              resourceAllocationEvent.getNodeId());
          final String nodeId = resourceAllocationEvent.getNodeId();
          final String[] hostNameAndPort = nodeId.split(":");
          Validate.isTrue(hostNameAndPort.length == 2);
          final String[] rackAndNumber = hostNameAndPort[0].split("-");
          Validate.isTrue(rackAndNumber.length == 2);
          final NodeDescriptorEvent event = NodeDescriptorEventImpl.newBuilder().setIdentifier(nodeId)
              .setHostName(hostNameAndPort[0]).setPort(Integer.parseInt(hostNameAndPort[1]))
              .setMemorySize(resourceAllocationEvent.getResourceMemory()).setRackName(rackAndNumber[0]).build();
          // downcast not to change the API
          Validate.isTrue(this.resourceCatalog instanceof ResourceCatalogImpl);
          // add the nodeDescriptor
          ((ResourceCatalogImpl) this.resourceCatalog).handle(event);
          // request it again
          nodeDescriptor = this.resourceCatalog.getNode(resourceAllocationEvent.getNodeId());
        } else {
          throw new RuntimeException("Unknown resource: " + resourceAllocationEvent.getNodeId());
        }
      } else {
        throw new RuntimeException("Unknown resource: " + resourceAllocationEvent.getNodeId());
      }
    }
    final EvaluatorDescriptorImpl evaluatorDescriptor = new EvaluatorDescriptorImpl(nodeDescriptor,
        resourceAllocationEvent.getResourceMemory(), resourceAllocationEvent.getVirtualCores().get(),
        processFactory.newEvaluatorProcess());

    LOG.log(Level.FINEST, "Resource allocation: new evaluator id[{0}]", resourceAllocationEvent.getIdentifier());
    final EvaluatorManager evaluatorManager =
        getNewEvaluatorManagerInstance(resourceAllocationEvent.getIdentifier(), evaluatorDescriptor);
    evaluatorManager.fireEvaluatorAllocatedEvent();

    return evaluatorManager;
  }

  /**
   * Instantiates a new EvaluatorManager for a failed evaluator during driver restart.
   * Does not fire an EvaluatorAllocatedEvent.
   * @param resourceStatusEvent
   * @return an EvaluatorManager for the user to call fail on.
   */
  public EvaluatorManager createForEvaluatorFailedDuringDriverRestart(final ResourceStatusEvent resourceStatusEvent) {
    if (!resourceStatusEvent.getIsFromPreviousDriver().get()) {
      throw new RuntimeException("Invalid resourceStatusEvent, must be status for resource from previous Driver.");
    }
    return getNewEvaluatorManagerInstance(resourceStatusEvent.getIdentifier(),
        new EvaluatorDescriptorImpl(null, 128, 1, processFactory.newEvaluatorProcess()));
  }
}
