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
package org.apache.reef.io.network.group.impl.driver;

import org.apache.reef.io.network.group.api.driver.TaskNode;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.io.network.proto.ReefNetworkGroupCommProtos;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;

import java.util.List;
import java.util.logging.Logger;

/**
 *
 */
public class TopologyUpdateWaitHandler implements EventHandler<List<TaskNode>> {

  private static final Logger LOG = Logger.getLogger(TopologyUpdateWaitHandler.class.getName());
  private final EStage<GroupCommunicationMessage> senderStage;
  private final Class<? extends Name<String>> groupName;
  private final Class<? extends Name<String>> operName;
  private final String driverId;
  private final int driverVersion;
  private final String dstId;
  private final int dstVersion;
  private final String qualifiedName;


  /**
   * The handler will wait for all nodes to acquire topoLock
   * and send TopologySetup msg. Then it will send TopologyUpdated
   * msg. However, any local topology changes are not in effect
   * till driver sends TopologySetup once statusMap is emptied
   * The operations in the tasks that have topology changes will
   * wait for this. However other tasks that do not have any changes
   * will continue their regular operation
   */
  public TopologyUpdateWaitHandler(final EStage<GroupCommunicationMessage> senderStage,
                                   final Class<? extends Name<String>> groupName,
                                   final Class<? extends Name<String>> operName,
                                   final String driverId, final int driverVersion,
                                   final String dstId, final int dstVersion,
                                   final String qualifiedName) {
    super();
    this.senderStage = senderStage;
    this.groupName = groupName;
    this.operName = operName;
    this.driverId = driverId;
    this.driverVersion = driverVersion;
    this.dstId = dstId;
    this.dstVersion = dstVersion;
    this.qualifiedName = qualifiedName;
  }


  @Override
  public void onNext(final List<TaskNode> nodes) {
    LOG.entering("TopologyUpdateWaitHandler", "onNext", new Object[]{qualifiedName, nodes});

    for (final TaskNode node : nodes) {
      LOG.fine(qualifiedName + "Waiting for " + node + " to enter TopologyUdate phase");
      node.waitForTopologySetupOrFailure();
      if (node.isRunning()) {
        LOG.fine(qualifiedName + node + " is in TopologyUpdate phase");
      } else {
        LOG.fine(qualifiedName + node + " has failed");
      }
    }
    LOG.finest(qualifiedName + "NodeTopologyUpdateWaitStage All to be updated nodes " + "have received TopologySetup");
    LOG.fine(qualifiedName + "All affected parts of the topology are in TopologyUpdate phase. Will send a note to ("
        + dstId + "," + dstVersion + ")");
    senderStage.onNext(Utils.bldVersionedGCM(groupName, operName,
        ReefNetworkGroupCommProtos.GroupCommMessage.Type.TopologyUpdated, driverId, driverVersion, dstId,
        dstVersion, Utils.EMPTY_BYTE_ARR));
    LOG.exiting("TopologyUpdateWaitHandler", "onNext", qualifiedName);
  }

}
