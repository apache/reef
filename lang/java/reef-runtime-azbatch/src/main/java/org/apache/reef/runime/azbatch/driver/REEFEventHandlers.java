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
package org.apache.reef.runime.azbatch.driver;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.driver.api.RuntimeParameters;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceAllocationEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.RuntimeStatusEvent;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

/**
 * Helper that represents the REEF layer to the Azure Batch runtime.
 */
@Private
public final class REEFEventHandlers implements AutoCloseable {
  private final EventHandler<ResourceAllocationEvent> resourceAllocationHandler;
  private final EventHandler<ResourceStatusEvent> resourceStatusHandler;
  private final EventHandler<RuntimeStatusEvent> runtimeStatusHandler;
  private final EventHandler<NodeDescriptorEvent> nodeDescriptorEventHandler;

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
   * @param nodeDescriptorProto
   */
  void onNodeDescriptor(final NodeDescriptorEvent nodeDescriptorProto) {
    this.nodeDescriptorEventHandler.onNext(nodeDescriptorProto);
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

  @Override
  public void close() throws Exception {
    // Empty, but here for a future where we need to close a threadpool
  }
}
