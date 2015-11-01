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
 * Event from Driver Runtime to Driver Process.
 * Description of a node in the cluster
 */
@RuntimeAuthor
@DriverSide
@DefaultImplementation(NodeDescriptorEventImpl.class)
public interface NodeDescriptorEvent {

  /**
   * @return Id of the node
   */
  String getIdentifier();

  /**
   * @return Host name of the node
   */
  String getHostName();

  /**
   * @return Port of the node
   */
  int getPort();

  /**
   * @return Total memory size of the node, in MB
   */
  int getMemorySize();

  /**
   * @return Name of rack where the node is located
   */
  Optional<String> getRackName();
}
