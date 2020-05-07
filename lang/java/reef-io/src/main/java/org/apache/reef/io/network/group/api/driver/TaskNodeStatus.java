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
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage.Type;

/**
 * Tracks the Status of the ctrl msgs sent to a
 * task node in the topology -
 *   what msgs have been sent to this node and
 *   what msgs have been ACKed as received by this node
 *   Status of neighbors
 * This is used to see whether the local topology
 * of a Task is completely set-up
 * It also offers convenience methods for waiting
 * on receiving ACKs from the task.
 */
public interface TaskNodeStatus {

  boolean hasChanges();

  void onTopologySetupMessageSent();

  boolean isActive(String neighborId);

  /**
   * Process the msg that was received and update.
   * state accordingly
   */
  void processAcknowledgement(GroupCommunicationMessage msg);

  /**
   * To be called before sending a ctrl msg to the task
   * represented by this node. All ctrl msgs sent to this
   * node need to be ACKed.
   * Ctrl msgs will be sent        from a src and
   * ACK sent from the task will be for a src.
   * As this is called from the TaskNodeImpl use srcId of msg
   * In TaskNodeImpl while processMsg        use dstId of msg
   */
  void expectAckFor(Type msgType, String srcId);

  /**
   * Used when the task has failed to clear all.
   * the state that is associated with this task
   * Also should release the locks held for implementing
   * the convenience wait* methods
   */
  void clearStateAndReleaseLocks();

  /**
   * This should remove state concerning neighboring tasks.
   * that have failed
   */
  void updateFailureOf(String taskId);

  void waitForTopologySetup();

  /**
   * Called to denote that a UpdateTopology msg will.
   * be sent
   */
  void updatingTopology();
}
