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
package org.apache.reef.io.network.group.api.task;

import org.apache.reef.annotations.Provided;
import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.io.network.group.impl.task.GroupCommClientImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.tang.annotations.Name;


/**
 * The task side interface for the Group Communication Service.
 */
@TaskSide
@Provided
@DefaultImplementation(value = GroupCommClientImpl.class)
public interface GroupCommClient {

  /**
   * @param groupName a group name
   * @return The communication group client with the given name that gives access
   * to the operators configured on it that will be used to do group communication
   */
  CommunicationGroupClient getCommunicationGroup(Class<? extends Name<String>> groupName);
}
