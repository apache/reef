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

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.RuntimeAuthor;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.util.Optional;

import java.util.List;

/**
 * Event from Driver Process to Driver Runtime.
 * A request to the Driver Runtime to allocate resources with the given specification
 */
@RuntimeAuthor
@DriverSide
@DefaultImplementation(ResourceRequestEventImpl.class)
public interface ResourceRequestEvent {

  /**
   * @return The number of resources requested
   */
  int getResourceCount();

  /**
   * @return A list of preferred nodes to place the resource on.
   *   An empty list indicates all nodes are equally preferred.
   */
  List<String> getNodeNameList();

  /**
   * @return A list of preferred racks to place the resource on,
   *   An empty list indicates all racks are equally preferred.
   */
  List<String> getRackNameList();

  /**
   * @return The amount of memory to allocate to the resource
   */
  Optional<Integer> getMemorySize();

  /**
   * @return The priority assigned to the resource
   */
  Optional<Integer> getPriority();

  /**
   * @return The number of virtual CPU cores to allocate for the resource
   */
  Optional<Integer> getVirtualCores();

  /**
   * @return If true, allow allocation on nodes and racks other than
   *   the preferred list. If false, strictly enforce the preferences.
   */
  Optional<Boolean> getRelaxLocality();

  /**
   * @return The runtime name
   */
  Optional<String> getRuntimeName();
}
