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

import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.io.network.group.api.operators.Broadcast;
import org.apache.reef.io.network.group.api.operators.Gather;
import org.apache.reef.io.network.group.api.operators.Reduce;
import org.apache.reef.io.network.group.api.GroupChanges;
import org.apache.reef.io.network.group.api.operators.Scatter;
import org.apache.reef.io.network.group.impl.driver.TopologySimpleNode;
import org.apache.reef.io.network.group.impl.task.CommunicationGroupClientImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.wake.Identifier;

import java.util.List;

/**
 * The Task side interface of a communication group.
 * Lets one get the operators configured for this task
 * and use them for communication between tasks configured
 * in this communication group
 */
@TaskSide
@DefaultImplementation(value = CommunicationGroupClientImpl.class)
public interface CommunicationGroupClient {

  /**
   * @return The name configured on this communication group
   */
  Class<? extends Name<String>> getName();

  /**
   * The broadcast sender configured on this communication group.
   * with the given oepratorName
   *
   * @param operatorName
   * @return
   */
  Broadcast.Sender getBroadcastSender(Class<? extends Name<String>> operatorName);

  /**
   * The broadcast receiver configured on this communication group.
   * with the given oepratorName
   *
   * @param operatorName
   * @return
   */
  Broadcast.Receiver getBroadcastReceiver(Class<? extends Name<String>> operatorName);

  /**
   * The reduce receiver configured on this communication group.
   * with the given operatorName
   *
   * @param operatorName
   * @return
   */
  Reduce.Receiver getReduceReceiver(Class<? extends Name<String>> operatorName);

  /**
   * The reduce sender configured on this communication group.
   * with the given operatorName
   *
   * @param operatorName
   * @return
   */
  Reduce.Sender getReduceSender(Class<? extends Name<String>> operatorName);

  /**
   * Return the scatter sender configured on this communication group.
   * {@code operatorName} is used to specify the scatter sender to return.
   *
   * @param operatorName
   * @return
   */
  Scatter.Sender getScatterSender(Class<? extends Name<String>> operatorName);

  /**
   * Return the scatter receiver configured on this communication group.
   * {@code operatorName} is used to specify the scatter receiver to return.
   *
   * @param operatorName
   * @return
   */
  Scatter.Receiver getScatterReceiver(Class<? extends Name<String>> operatorName);

  /**
   * Return the gather receiver configured on this communication group.
   * {@code operatorName} is used to specify the gather receiver to return.
   *
   * @param operatorName
   * @return
   */
  Gather.Receiver getGatherReceiver(Class<? extends Name<String>> operatorName);

  /**
   * Return the gather sender configured on this communication group.
   * {@code operatorName} is used to specify the gather sender to return.
   *
   * @param operatorName
   * @return
   */
  Gather.Sender getGatherSender(Class<? extends Name<String>> operatorName);

  /**
   * @return Changes in topology of this communication group since the last time
   * this method was called
   */
  GroupChanges getTopologyChanges();

  /**
   * @return list of current active tasks, last updated during updateTopology()
   */
  List<Identifier> getActiveSlaveTasks();

  /**
   * @return root node of simplified topology representation
   */
  TopologySimpleNode getTopologySimpleNodeRoot();

  /**
   * Asks the driver to update the topology of this communication group. This can
   * be an expensive call depending on what the minimum number of tasks is for this
   * group to function as this first tells the driver, driver then tells the affected
   * tasks and the driver gives a green only after affected tasks have had a chance
   * to be sure that their topology will be updated before the next message is
   * communicated
   */
  void updateTopology();


}
