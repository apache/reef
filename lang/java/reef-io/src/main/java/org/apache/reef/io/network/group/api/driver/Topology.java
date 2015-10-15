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

import org.apache.reef.io.network.group.api.config.OperatorSpec;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.tang.Configuration;

/**
 * A topology should implement the following
 * interface so that it can work with the
 * elastic group communication framework
 * Currently we have two implementations
 * 1. Flat 2. Tree
 */
public interface Topology {

  /**
   * Get the version of the Task 'taskId'.
   * that belongs to this topology
   *
   * @param taskId
   * @return
   */
  int getNodeVersion(String taskId);

  /**
   * Get the id of the root task.
   *
   * @return
   */
  String getRootId();

  /**
   * Check whether the root node has been added or not.
   *
   * @return {@code true} if root has been added, {@code false} otherwise
   */
  boolean isRootPresent();

  /**
   * Set task with id 'senderId' as.
   * the root of this topology
   *
   * @param senderId
   */
  void setRootTask(String senderId);

  /**
   * Add task with id 'taskId' to.
   * the topology
   *
   * @param taskId
   */
  void addTask(String taskId);

  /**
   * Remove task with id 'taskId' from.
   * the topology
   *
   * @param taskId
   */
  void removeTask(String taskId);

  /**
   * Update state on receipt of RunningTask.
   * event for task with id 'id'
   *
   * @param id
   */
  void onRunningTask(String id);

  /**
   * Update state on receipt of FailedTask.
   * event for task with id 'id'
   *
   * @param id
   */
  void onFailedTask(String id);

  /**
   * Set operator specification of the operator.
   * that is the owner of this topology instance
   *
   * @param spec
   */
  void setOperatorSpecification(OperatorSpec spec);

  /**
   * Get the topology portion of the Configuration.
   * for the task 'taskId' that belongs to this
   * topology
   *
   * @param taskId
   * @return
   */
  Configuration getTaskConfiguration(String taskId);

  /**
   * Update state on receipt of a message.
   * from the tasks
   *
   * @param msg
   */
  void onReceiptOfMessage(GroupCommunicationMessage msg);
}
