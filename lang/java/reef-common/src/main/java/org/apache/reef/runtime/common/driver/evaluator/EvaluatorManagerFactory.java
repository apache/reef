/**
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
import org.apache.reef.driver.evaluator.EvaluatorType;
import org.apache.reef.proto.DriverRuntimeProtocol;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorHandler;
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

  @Inject
  EvaluatorManagerFactory(final Injector injector, final ResourceCatalog resourceCatalog, final NodeDescriptorHandler nodeDescriptorHandler) {
    this.injector = injector;
    this.resourceCatalog = resourceCatalog;
  }

  /**
   * Helper method to create a new EvaluatorManager instance
   *
   * @param id   identifier of the Evaluator
   * @param desc NodeDescriptor on which the Evaluator executes.
   * @return a new EvaluatorManager instance.
   */
  private final EvaluatorManager getNewEvaluatorManagerInstance(final String id, final EvaluatorDescriptorImpl desc) {
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
   * @param resourceAllocationProto
   * @return
   */
  public final EvaluatorManager getNewEvaluatorManager(final DriverRuntimeProtocol.ResourceAllocationProto resourceAllocationProto) {
    final NodeDescriptor nodeDescriptor = this.resourceCatalog.getNode(resourceAllocationProto.getNodeId());

    if (nodeDescriptor == null) {
      throw new RuntimeException("Unknown resource: " + resourceAllocationProto.getNodeId());
    }
    final EvaluatorDescriptorImpl evaluatorDescriptor = new EvaluatorDescriptorImpl(nodeDescriptor,
        EvaluatorType.UNDECIDED, resourceAllocationProto.getResourceMemory(), resourceAllocationProto.getVirtualCores());

    LOG.log(Level.FINEST, "Resource allocation: new evaluator id[{0}]", resourceAllocationProto.getIdentifier());
    return this.getNewEvaluatorManagerInstance(resourceAllocationProto.getIdentifier(), evaluatorDescriptor);
  }

  public final EvaluatorManager createForEvaluatorFailedDuringDriverRestart(final DriverRuntimeProtocol.ResourceStatusProto resourceStatusProto) {
    if (!resourceStatusProto.getIsFromPreviousDriver()) {
      throw new RuntimeException("Invalid resourceStatusProto, must be status for resource from previous Driver.");
    }
    return getNewEvaluatorManagerInstance(resourceStatusProto.getIdentifier(), new EvaluatorDescriptorImpl(null, EvaluatorType.UNDECIDED, 128, 1));
  }
}
