/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.runtime.yarn.driver;

import com.microsoft.reef.annotations.audience.Private;
import com.microsoft.reef.proto.DriverRuntimeProtocol;
import com.microsoft.reef.runtime.common.driver.api.RuntimeParameters;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;

/**
 * Helper that represents the REEF layer to the YARN runtime.
 */
// This is a great place to add a thread boundary, should that need arise.
@Private
final class REEFEventHandlers implements AutoCloseable {
  private final EventHandler<DriverRuntimeProtocol.ResourceAllocationProto> resourceAllocationHandler;
  private final EventHandler<DriverRuntimeProtocol.ResourceStatusProto> resourceStatusHandler;
  private final EventHandler<DriverRuntimeProtocol.RuntimeStatusProto> runtimeStatusHandler;
  private final EventHandler<DriverRuntimeProtocol.NodeDescriptorProto> nodeDescriptorProtoEventHandler;

  @Inject
  REEFEventHandlers(final @Parameter(RuntimeParameters.NodeDescriptorHandler.class) EventHandler<DriverRuntimeProtocol.NodeDescriptorProto> nodeDescriptorProtoEventHandler,
                    final @Parameter(RuntimeParameters.RuntimeStatusHandler.class) EventHandler<DriverRuntimeProtocol.RuntimeStatusProto> runtimeStatusProtoEventHandler,
                    final @Parameter(RuntimeParameters.ResourceAllocationHandler.class) EventHandler<DriverRuntimeProtocol.ResourceAllocationProto> resourceAllocationHandler,
                    final @Parameter(RuntimeParameters.ResourceStatusHandler.class) EventHandler<DriverRuntimeProtocol.ResourceStatusProto> resourceStatusHandler) {
    this.resourceAllocationHandler = resourceAllocationHandler;
    this.resourceStatusHandler = resourceStatusHandler;
    this.runtimeStatusHandler = runtimeStatusProtoEventHandler;
    this.nodeDescriptorProtoEventHandler = nodeDescriptorProtoEventHandler;
  }

  /**
   * Inform reef of a node.
   *
   * @param nodeDescriptorProto
   */
  void onNodeDescriptor(final DriverRuntimeProtocol.NodeDescriptorProto nodeDescriptorProto) {
    this.nodeDescriptorProtoEventHandler.onNext(nodeDescriptorProto);
  }

  /**
   * Update REEF's view on the runtime status.
   *
   * @param runtimeStatusProto
   */
  void onRuntimeStatus(final DriverRuntimeProtocol.RuntimeStatusProto runtimeStatusProto) {
    this.runtimeStatusHandler.onNext(runtimeStatusProto);
  }

  /**
   * Inform REEF of a fresh resource allocation.
   *
   * @param resourceAllocationProto
   */
  void onResourceAllocation(final DriverRuntimeProtocol.ResourceAllocationProto resourceAllocationProto) {
    this.resourceAllocationHandler.onNext(resourceAllocationProto);
  }

  /**
   * Update REEF on a change to the status of a resource.
   *
   * @param resourceStatusProto
   */
  void onResourceStatus(final DriverRuntimeProtocol.ResourceStatusProto resourceStatusProto) {
    this.resourceStatusHandler.onNext(resourceStatusProto);
  }

  @Override
  public void close() throws Exception {
    // Empty, but here for a future where we need to close a threadpool
  }
}
