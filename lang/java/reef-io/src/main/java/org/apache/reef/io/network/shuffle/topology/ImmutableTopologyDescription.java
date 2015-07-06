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
package org.apache.reef.io.network.shuffle.topology;

import org.apache.reef.io.network.shuffle.driver.ShuffleTopologyManager;
import org.apache.reef.io.network.shuffle.impl.ImmutableShuffleTopologyClient;
import org.apache.reef.io.network.shuffle.impl.ImmutableShuffleTopologyManager;
import org.apache.reef.io.network.shuffle.task.ShuffleTopologyClient;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Name;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public final class ImmutableTopologyDescription implements TopologyDescription {

  private final Class<? extends Name<String>> topologyName;
  private final Map<String, NodePoolDescription> nodePoolDescriptionMap;
  private final List<GroupingDescription> groupingDescriptionList;
  private final Class<? extends ShuffleTopologyManager> managerClass;
  private final Configuration managerConfiguration;
  private final Class<? extends ShuffleTopologyClient> clientClass;

  private ImmutableTopologyDescription(
      final Class<? extends Name<String>> topologyName,
      final Map<String, NodePoolDescription> nodePoolDescriptionMap,
      final List<GroupingDescription> groupingDescriptionList,
      final Class<? extends ShuffleTopologyManager> managerClass,
      final Configuration managerConfiguration,
      final Class<? extends ShuffleTopologyClient> clientClass) {
    this.topologyName = topologyName;
    this.nodePoolDescriptionMap = nodePoolDescriptionMap;
    this.groupingDescriptionList = groupingDescriptionList;
    this.managerClass = managerClass;
    this.managerConfiguration = managerConfiguration;
    this.clientClass = clientClass;
  }

  @Override
  public Class<? extends Name<String>> getTopologyName() {
    return topologyName;
  }

  @Override
  public Map<String, NodePoolDescription> getNodePoolDescriptionMap() {
    return nodePoolDescriptionMap;
  }

  @Override
  public List<GroupingDescription> getGroupingDescriptionList() {
    return groupingDescriptionList;
  }

  @Override
  public Class<? extends ShuffleTopologyManager> getManagerClass() {
    return managerClass;
  }

  @Override
  public Configuration getManagerConfiguration() {
    return managerConfiguration;
  }

  @Override
  public Class<? extends ShuffleTopologyClient> getClientClass() {
    return clientClass;
  }

  public static Builder newBuilder(Class<? extends Name<String>> topologyName) {
    return new Builder(topologyName);
  }

  public static class Builder {

    private final Class<? extends Name<String>> topologyName;
    private final Map<String, NodePoolDescription> nodePoolDescriptionMap;
    private final List<GroupingDescription> groupingDescriptionList;
    private Class<? extends ShuffleTopologyManager> managerClass;
    private Configuration managerConfiguration;
    private Class<? extends ShuffleTopologyClient> clientClass;

    private Builder(Class<? extends Name<String>> topologyName) {
      this.topologyName = topologyName;
      this.nodePoolDescriptionMap = new HashMap<>();
      this.groupingDescriptionList = new ArrayList<>();
      this.managerClass = ImmutableShuffleTopologyManager.class;
      this.clientClass = ImmutableShuffleTopologyClient.class;
    }

    public Builder addNodePoolDescription(final NodePoolDescription nodePoolDescription) {
      final String nodePoolName = nodePoolDescription.getNodePoolName();
      if (nodePoolDescriptionMap.containsKey(nodePoolName)) {
        throw new RuntimeException(nodePoolName + " was already added.");
      }
      nodePoolDescriptionMap.put(nodePoolName, nodePoolDescription);
      return this;
    }

    public Builder addGroupingDescription(final GroupingDescription groupingDescription) {
      if (!nodePoolDescriptionMap.containsKey(groupingDescription.getReceiverPoolId())) {
        throw new RuntimeException("You have to add node pool named "
            + groupingDescription.getReceiverPoolId() + " for receiver first.");
      }

      if (!nodePoolDescriptionMap.containsKey(groupingDescription.getSenderPoolId())) {
        throw new RuntimeException("You have to add node pool named "
            + groupingDescription.getSenderPoolId() + " for sender first.");
      }
      groupingDescriptionList.add(groupingDescription);
      return this;
    }

    public Builder setManagerClass(final Class<? extends ShuffleTopologyManager> managerClass) {
      this.managerClass = managerClass;
      return this;
    }

    public Builder setManagerConfiguration(final Configuration managerConfiguration) {
      this.managerConfiguration = managerConfiguration;
      return this;
    }

    public Builder setClientClass(final Class<? extends ShuffleTopologyClient> clientClass) {
      this.clientClass = clientClass;
      return this;
    }

    public TopologyDescription build() {
      return new ImmutableTopologyDescription(
          topologyName,
          nodePoolDescriptionMap,
          groupingDescriptionList,
          managerClass,
          managerConfiguration,
          clientClass
      );
    }
  }
}
