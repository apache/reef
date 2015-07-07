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

import org.apache.reef.tang.annotations.Name;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public final class ShuffleDescription implements ShuffleDescriptor {

  private final Class<? extends Name<String>> shuffleName;
  private final Map<String, List<String>> senderIdListMap;
  private final Map<String, List<String>> receiverIdListMap;
  private final Map<String, GroupingDescriptor> groupingDescriptorMap;
  private final List<String> groupingNameList;

  private ShuffleDescription(
      final Class<? extends Name<String>> shuffleName,
      final Map<String, List<String>> senderIdListMap,
      final Map<String, List<String>> receiverIdListMap,
      final Map<String, GroupingDescriptor> groupingDescriptorMap,
      final List<String> groupingNameList) {
    this.shuffleName = shuffleName;
    this.senderIdListMap = senderIdListMap;
    this.receiverIdListMap = receiverIdListMap;
    this.groupingDescriptorMap = groupingDescriptorMap;
    this.groupingNameList = groupingNameList;
  }

  @Override
  public Class<? extends Name<String>> getShuffleName() {
    return shuffleName;
  }

  @Override
  public List<String> getGroupingNameList() {
    return groupingNameList;
  }

  @Override
  public GroupingDescriptor getGroupingDescriptor(final String groupingName) {
    return groupingDescriptorMap.get(groupingName);
  }

  @Override
  public List<String> getSenderIdList(final String groupingName) {
    return senderIdListMap.get(groupingName);
  }

  @Override
  public List<String> getReceiverIdList(final String groupingName) {
    return receiverIdListMap.get(groupingName);
  }

  public static Builder newBuilder(final Class<? extends Name<String>> shuffleName) {
    return new Builder(shuffleName);
  }

  public static class Builder {

    private final Class<? extends Name<String>> shuffleName;
    private final Map<String, List<String>> senderIdListMap;
    private final Map<String, List<String>> receiverIdListMap;
    private final Map<String, GroupingDescriptor> groupingDescriptorMap;
    private final List<String> groupingNameList;

    private Builder(Class<? extends Name<String>> shuffleName) {
      this.shuffleName = shuffleName;
      this.senderIdListMap = new HashMap<>();
      this.receiverIdListMap = new HashMap<>();
      this.groupingDescriptorMap = new HashMap<>();
      this.groupingNameList = new ArrayList<>();
    }

    public Builder addGrouping(final List<String> senderIdList, final List<String> receiverIdList, final GroupingDescriptor groupingDescriptor) {
      if (groupingDescriptorMap.containsKey(groupingDescriptor.getGroupingName())) {
        throw new RuntimeException(groupingDescriptor.getGroupingName() + " was already added.");
      }

      groupingDescriptorMap.put(groupingDescriptor.getGroupingName(), groupingDescriptor);
      groupingNameList.add(groupingDescriptor.getGroupingName());
      senderIdListMap.put(groupingDescriptor.getGroupingName(), senderIdList);
      receiverIdListMap.put(groupingDescriptor.getGroupingName(), receiverIdList);
      return this;
    }

    public ShuffleDescription build() {
      return new ShuffleDescription(
          shuffleName,
          senderIdListMap,
          receiverIdListMap,
          groupingDescriptorMap,
          groupingNameList
      );
    }
  }
}
