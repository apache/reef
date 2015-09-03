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
package org.apache.reef.io.network.group.api.driver;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.io.network.group.impl.driver.GroupCommDriverImpl;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.tang.annotations.Name;

/**
 * The driver side interface of Group Communication.
 * which is the entry point for the service
 */
@DriverSide
@Provided
@DefaultImplementation(value = GroupCommDriverImpl.class)
public interface GroupCommDriver {

  /**
   * Create a new communication group with the specified name.
   * and the minimum number of tasks needed in this group before
   * communication can start
   *
   * @param groupName
   * @param numberOfTasks
   * @return
   */
  CommunicationGroupDriver newCommunicationGroup(Class<? extends Name<String>> groupName, int numberOfTasks);

  /**
   * Create a new communication group with the specified name,
   * the minimum number of tasks needed in this group before
   * communication can start, and a custom fanOut.
   *
   * @param groupName
   * @param numberOfTasks
   * @param customFanOut
   * @return
   */
  CommunicationGroupDriver newCommunicationGroup(Class<? extends Name<String>> groupName, int numberOfTasks,
      int customFanOut);

  /**
   * Tests whether the activeContext is a context configured.
   * using the Group Communication Service
   *
   * @param activeContext
   * @return
   */
  boolean isConfigured(ActiveContext activeContext);

  /**
   * @return Configuration needed for a Context that should have
   * Group Communication Service enabled
   */
  Configuration getContextConfiguration();

  /**
   * @return Configuration needed to enable
   * Group Communication as a Service
   */
  Configuration getServiceConfiguration();

  /**
   * @return Configuration needed for a Task that should have
   * Group Communication Service enabled
   */
  Configuration getTaskConfiguration(Configuration partialTaskConf);

}
