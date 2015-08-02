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

import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;

/**
 * A node in the Topology representing a Task on the driver.
 * Impl should maintain state relating to whether task is running/dead and
 * status of neighboring nodes and send ctrl msgs to the tasks indicating
 * topology changing events
 */
public interface TaskNode {

  String getTaskId();

  int getVersion();

  int getNumberOfChildren();

  TaskNode getParent();

  void setParent(TaskNode parent);

  Iterable<TaskNode> getChildren();

  void addChild(TaskNode child);

  void removeChild(TaskNode taskNode);

  boolean isRunning();

  void onRunningTask();

  void onFailedTask();

  boolean hasChanges();

  boolean isNeighborActive(String neighborId);

  void onReceiptOfAcknowledgement(GroupCommunicationMessage msg);

  void onParentRunning();

  void onParentDead();

  void onChildRunning(String childId);

  void onChildDead(String childId);

  /**
   * Check if this node is ready for sending.
   * TopologySetup
   */
  void checkAndSendTopologySetupMessage();

  /**
   * Check if the neighbor node with id source.
   * is ready for sending TopologySetup
   * @param source
   */
  void checkAndSendTopologySetupMessageFor(String source);

  /**
   * reset topology setup ensures that update topology is not sent to someone.
   * who is already updating topology which is usually when they are just
   * (re)starting
   *
   * @return
   */
  boolean resetTopologySetupSent();

  void waitForTopologySetupOrFailure();

  void setSibling(TaskNode leaf);

  TaskNode successor();

  void updatingTopology();
}
