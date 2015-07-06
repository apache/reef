/**
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
package org.apache.reef.io.network.shuffle.utils;

import org.apache.reef.io.network.shuffle.params.*;
import org.apache.reef.io.network.shuffle.task.ShuffleTopologyClient;
import org.apache.reef.io.network.shuffle.topology.GroupingDescription;
import org.apache.reef.io.network.shuffle.topology.NodePoolDescription;
import org.apache.reef.io.network.shuffle.topology.TopologyDescription;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public final class TopologyConfigurationSerializer {

  private final String taskId;
  private final TopologyDescription topologyDescription;
  private final Map<String, NodePoolDescription> nodePoolMap;
  private final List<NodePoolDescription> nodePoolDescriptionListHasTask;
  private final List<GroupingDescription> groupingDescriptionList;
  private final ConfigurationSerializer confSerializer;
  private final JavaConfigurationBuilder confBuilder;

  public TopologyConfigurationSerializer(
      final String taskId,
      final TopologyDescription topologyDescription,
      final ConfigurationSerializer confSerializer) {
    this.taskId = taskId;
    this.topologyDescription = topologyDescription;
    this.nodePoolMap = topologyDescription.getNodePoolDescriptionMap();
    this.groupingDescriptionList = topologyDescription.getGroupingDescriptionList();
    this.confSerializer = confSerializer;
    this.confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    this.nodePoolDescriptionListHasTask = new ArrayList<>();
    getNodePoolDescriptionListHasTask();
  }

  private void getNodePoolDescriptionListHasTask() {
    for (final NodePoolDescription nodePoolDescription : nodePoolMap.values()) {
      if (nodePoolDescription.hasNodeId(taskId)) {
        nodePoolDescriptionListHasTask.add(nodePoolDescription);
      }
    }
  }

  public Configuration getConfiguration() {
    if (nodePoolDescriptionListHasTask.size() == 0) {
      return null;
    }

    confBuilder.bindNamedParameter(ShuffleTopologyName.class, topologyDescription.getTopologyName().getName());
    confBuilder.bindImplementation(ShuffleTopologyClient.class, topologyDescription.getClientClass());
    for (final NodePoolDescription description : nodePoolDescriptionListHasTask) {
      confBuilder.bindSetEntry(SerializedNodePoolSet.class,
          confSerializer.toString(description.convertToConfiguration()));
    }

    for (final GroupingDescription description : groupingDescriptionList) {
      addGroupingDescription(description);
    }

    return confBuilder.build();
  }

  private void addGroupingDescription(final GroupingDescription groupingDescription) {
    final NodePoolDescription senderPool = nodePoolMap.get(groupingDescription.getSenderPoolId());
    final NodePoolDescription receiverPool = nodePoolMap.get(groupingDescription.getReceiverPoolId());
    if (!senderPool.hasNodeId(taskId) && !receiverPool.hasNodeId(taskId)) {
      return;
    }

    confBuilder.bindSetEntry(SerializedSenderNodePoolSet.class,
        confSerializer.toString(senderPool.convertToConfiguration()));

    confBuilder.bindSetEntry(SerializedReceiverNodePoolSet.class,
        confSerializer.toString(receiverPool.convertToConfiguration()));

    confBuilder.bindSetEntry(SerializedGroupingSet.class,
        confSerializer.toString(groupingDescription.convertToConfiguration()));
  }
}
