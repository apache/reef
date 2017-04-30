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
package org.apache.reef.runtime.spark.driver;

import org.apache.reef.annotations.audience.Private;
import org.apache.reef.runtime.common.driver.api.RuntimeParameters;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceAllocationEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.RuntimeStatusEvent;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;

@Private
final class REEFEventHandlers {
  private final EventHandler<ResourceAllocationEvent> resourceAllocationEventHandler;
  private final EventHandler<RuntimeStatusEvent> runtimeStatusEventHandler;
  private final EventHandler<NodeDescriptorEvent> nodeDescriptorEventHandler;
  private final EventHandler<ResourceStatusEvent> resourceStatusHandlerEventHandler;

  @Inject
  REEFEventHandlers(@Parameter(RuntimeParameters.ResourceAllocationHandler.class)
                    final EventHandler<ResourceAllocationEvent> resourceAllocationEventHandler,
                    @Parameter(RuntimeParameters.RuntimeStatusHandler.class)
                    final EventHandler<RuntimeStatusEvent> runtimeStatusEventHandler,
                    @Parameter(RuntimeParameters.NodeDescriptorHandler.class)
                    final EventHandler<NodeDescriptorEvent> nodeDescriptorEventHandler,
                    @Parameter(RuntimeParameters.ResourceStatusHandler.class)
                    final EventHandler<ResourceStatusEvent> resourceStatusHandlerEventHandler) {
    this.resourceAllocationEventHandler = resourceAllocationEventHandler;
    this.runtimeStatusEventHandler = runtimeStatusEventHandler;
    this.nodeDescriptorEventHandler = nodeDescriptorEventHandler;
    this.resourceStatusHandlerEventHandler = resourceStatusHandlerEventHandler;
  }

  void onNodeDescriptor(final NodeDescriptorEvent nodeDescriptorProto) {
    this.nodeDescriptorEventHandler.onNext(nodeDescriptorProto);
  }

  void onRuntimeStatus(final RuntimeStatusEvent runtimeStatusProto) {
    this.runtimeStatusEventHandler.onNext(runtimeStatusProto);
  }

  void onResourceAllocation(final ResourceAllocationEvent resourceAllocationProto) {
    this.resourceAllocationEventHandler.onNext(resourceAllocationProto);
  }

  void onResourceStatus(final ResourceStatusEvent resourceStatusProto) {
    this.resourceStatusHandlerEventHandler.onNext(resourceStatusProto);
  }
}
