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
package org.apache.reef.runtime.azbatch.driver;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.driver.api.RuntimeParameters;
import org.apache.reef.runtime.common.driver.resourcemanager.*;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * Helper that represents the REEF layer to the Azure Batch runtime.
 */
@Private
public final class REEFEventHandlers {
  private final EventHandler<ResourceAllocationEvent> resourceAllocationHandler;
  private final EventHandler<ResourceStatusEvent> resourceStatusHandler;
  private final EventHandler<RuntimeStatusEvent> runtimeStatusHandler;
  private final EventHandler<NodeDescriptorEvent> nodeDescriptorEventHandler;
  private static final Logger LOG = Logger.getLogger(REEFEventHandlers.class.getName());

  @Inject
  REEFEventHandlers(@Parameter(RuntimeParameters.NodeDescriptorHandler.class)
                    final EventHandler<NodeDescriptorEvent> nodeDescriptorEventHandler,
                    @Parameter(RuntimeParameters.RuntimeStatusHandler.class)
                    final EventHandler<RuntimeStatusEvent> runtimeStatusProtoEventHandler,
                    @Parameter(RuntimeParameters.ResourceAllocationHandler.class)
                    final EventHandler<ResourceAllocationEvent> resourceAllocationHandler,
                    @Parameter(RuntimeParameters.ResourceStatusHandler.class)
                    final EventHandler<ResourceStatusEvent> resourceStatusHandler) {
    this.resourceAllocationHandler = resourceAllocationHandler;
    this.resourceStatusHandler = resourceStatusHandler;
    this.runtimeStatusHandler = runtimeStatusProtoEventHandler;
    this.nodeDescriptorEventHandler = nodeDescriptorEventHandler;
  }

  /**
   * Inform reef of a node.
   *
   * @param nodeDescriptorEvent
   */
  void onNodeDescriptor(final NodeDescriptorEvent nodeDescriptorEvent) {
    this.nodeDescriptorEventHandler.onNext(nodeDescriptorEvent);
  }

  /**
   * Update REEF's view on the runtime status.
   *
   * @param runtimeStatusEvent
   */
  @Private
  public void onRuntimeStatus(final RuntimeStatusEvent runtimeStatusEvent) {
    this.runtimeStatusHandler.onNext(runtimeStatusEvent);
  }

  /**
   * Inform REEF of a fresh resource allocation.
   *
   * @param resourceAllocationEvent
   */
  @Private
  public void onResourceAllocation(final ResourceAllocationEvent resourceAllocationEvent) {
    this.resourceAllocationHandler.onNext(resourceAllocationEvent);
  }

  /**
   * Update REEF on a change to the status of a resource.
   *
   * @param resourceStatusEvent
   */
  void onResourceStatus(final ResourceStatusEvent resourceStatusEvent) {
    this.resourceStatusHandler.onNext(resourceStatusEvent);
  }
}
