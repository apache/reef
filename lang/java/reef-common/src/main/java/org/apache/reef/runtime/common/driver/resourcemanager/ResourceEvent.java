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
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.driver.evaluator.SchedulingConstraint;
import org.apache.reef.util.Optional;

/**
 * An interface capturing the characteristics of a resource event.
 */
@DriverSide
@Private
public interface ResourceEvent {

  /**
   * @return Id of the resource
   */
  String getIdentifier();

  /**
   * @return Memory size of the resource, in MB
   */
  int getResourceMemory();

  /**
   * @return Id of the node where resource is
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

  /**
   * @return Runtime name of the resource
   */
  String getRuntimeName();

  /**
   * @return Scheduling constraint of the resource.
   */
  Optional<SchedulingConstraint> getSchedulingConstraint();
}
