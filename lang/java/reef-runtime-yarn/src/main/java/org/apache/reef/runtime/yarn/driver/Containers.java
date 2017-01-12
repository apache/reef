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
package org.apache.reef.runtime.yarn.driver;


import org.apache.hadoop.yarn.api.records.Container;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.util.Optional;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Helper class that manages the set of Containers we know about.
 */
@Private
@DriverSide
final class Containers {
  private final Map<String, Container> containers = new ConcurrentHashMap<>();

  @Inject
  Containers() {
  }

  /**
   * @param containerID
   * @return the container for the given id.
   * @throws java.lang.RuntimeException when the container is unknown.
   */
  synchronized Container get(final String containerID) {
    final Container result = this.containers.get(containerID);
    if (null == result) {
      throw new RuntimeException("Requesting an unknown container: " + containerID);
    }
    return result;
  }

  /**
   * Registers the given container.
   *
   * @param container
   * @throws java.lang.RuntimeException if a container with the same ID had been registered before.
   */
  synchronized void add(final Container container) {
    final String containerId = container.getId().toString();
    if (this.hasContainer(containerId)) {
      throw new RuntimeException("Trying to add a Container that is already known: " + containerId);
    }
    this.containers.put(containerId, container);
  }

  /**
   * Removes the container with the given ID.
   *
   * @param containerId
   * @return the container that was registered before.
   * @throws java.lang.RuntimeException if no such container existed.
   */
  synchronized Container removeAndGet(final String containerId) {
    final Container result = this.containers.remove(containerId);
    if (null == result) {
      throw new RuntimeException("Unknown container to remove: " + containerId);
    }
    return result;
  }

  /**
   * @param containerId
   * @return true, if a container with this ID is known.
   */
  synchronized boolean hasContainer(final String containerId) {
    return this.containers.containsKey(containerId);
  }


  /**
   * @param containerId
   * @return the Container stored under this containerId or an empty Optional.
   */
  synchronized Optional<Container> getOptional(final String containerId) {
    return Optional.ofNullable(this.containers.get(containerId));
  }

  /**
   * @return an Iterable of all the known container Ids.
   */
  synchronized Iterable<String> getContainerIds() {
    return new ArrayList<>(this.containers.keySet());
  }
}
