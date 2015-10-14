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
package org.apache.reef.runtime.common.driver.api;

import org.apache.reef.annotations.audience.RuntimeAuthor;
import org.apache.reef.runtime.common.driver.resourcemanager.NodeDescriptorEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceAllocationEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.ResourceStatusEvent;
import org.apache.reef.runtime.common.driver.resourcemanager.RuntimeStatusEvent;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.wake.EventHandler;

/**
 * Driver resourcemanager parameters that resourcemanager implementations use to communicate with
 * the Driver.
 */
@RuntimeAuthor
public final class RuntimeParameters {
  /**
   * The resource allocation handler that stub runtimes send along allocated resources e.g., containers.
   */
  @NamedParameter(doc = "The resource allocation handler that stub runtimes send along allocated resources " +
      "e.g., containers.")
  public static final class ResourceAllocationHandler implements Name<EventHandler<ResourceAllocationEvent>> {
  }

  /**
   * The node descriptor handler that stub runtimes send along node information.
   */
  @NamedParameter(doc = "The node descriptor handler that stub runtimes send along node information.")
  public static final class NodeDescriptorHandler implements Name<EventHandler<NodeDescriptorEvent>> {
  }

  /**
   * The resource status handler.
   */
  @NamedParameter(doc = "The resource status handler.")
  public static final class ResourceStatusHandler implements Name<EventHandler<ResourceStatusEvent>> {
  }

  /**
   * The resourcemanager status handler.
   */
  @NamedParameter(doc = "The resourcemanager status handler.")
  public static final class RuntimeStatusHandler implements Name<EventHandler<RuntimeStatusEvent>> {
  }

}
