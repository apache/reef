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

import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;

/**
 *
 */
public final class ImmutableNodePoolDescription implements NodePoolDescription {

  private final String poolName;
  private final List<String> nodeIdList;

  @Inject
  private ImmutableNodePoolDescription(
      final @Parameter(NodePoolId.class) String poolName,
      final @Parameter(NodeIdSet.class) Set<String> nodeIdSet) {
    this.poolName = poolName;
    this.nodeIdList = new ArrayList<>(nodeIdSet);
    Collections.sort(nodeIdList);
  }

  @Override
  public String getNodePoolName() {
    return poolName;
  }

  @Override
  public List<String> getNodeIdList() {
    return nodeIdList;
  }

  @Override
  public int getNodePoolSize() {
    return nodeIdList.size();
  }

  @Override
  public String getNodeIdAt(int index) {
    return nodeIdList.get(index);
  }

  @Override
  public boolean hasNodeId(String nodeId) {
    for (final String id : nodeIdList) {
      if (id.equals(nodeId)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public Configuration convertToConfiguration() {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    confBuilder.bindNamedParameter(NodePoolId.class, poolName);
    confBuilder.bindImplementation(NodePoolDescription.class, ImmutableNodePoolDescription.class);
    for (final String nodeId : nodeIdList) {
      confBuilder.bindSetEntry(NodeIdSet.class, nodeId);
    }
    return confBuilder.build();
  }

  public static Builder newBuilder(final String poolName) {
    return new Builder(poolName);
  }

  public static class Builder {

    private final String poolName;
    private final Set<String> nodeIdSet;

    private Builder(final String poolName) {
      this.poolName = poolName;
      this.nodeIdSet = new HashSet<>();
    }

    public Builder addNodeIds(final Iterable<String> nodeIds) {
      Iterator<String> itr = nodeIds.iterator();
      while(itr.hasNext()) {
        addNodeId(itr.next());
      }
      return this;
    }

    public Builder addNodeIds(final String[] nodeIds) {
      for (int i = 0 ; i < nodeIds.length; i++) {
        addNodeId(nodeIds[i]);
      }
      return this;
    }

    public Builder addNodeId(final String nodeName) {
      if (!nodeIdSet.add(nodeName)) {
        throw new RuntimeException(nodeName + " was already added.");
      }
      return this;
    }

    public NodePoolDescription build() {
      return new ImmutableNodePoolDescription(poolName, nodeIdSet);
    }
  }

  @NamedParameter
  public static class NodePoolId implements Name<String> {
  }

  @NamedParameter
  public static class NodeIdSet implements Name<Set<String>> {
  }
}
