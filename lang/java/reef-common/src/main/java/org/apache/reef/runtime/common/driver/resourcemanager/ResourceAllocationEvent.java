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
package org.apache.reef.runtime.common.driver.resourcemanager;

import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.RuntimeAuthor;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.util.Optional;

/**
 * Event from Driver Runtime -> Driver Process
 * A Resource allocated by the Driver Runtime. In response to a ResourceRequestEvent.
 */
@RuntimeAuthor
@DriverSide
@DefaultImplementation(ResourceAllocationEventImpl.class)
public interface ResourceAllocationEvent {

  /**
   * @return Id of the allocated resource
   */
  String getIdentifier();

  /**
   * @return Memory size of the resource, in MB
   */
  int getResourceMemory();

  /**
   * @return Id of the node where resource was allocated
   */
  String getNodeId();

  /**
   * @return Number of virtual CPU cores on the resource
   */
  Optional<Integer> getVirtualCores();

  /**
   * @return Rack name of the resource
   */
  Optional<String> getRackName();

}
