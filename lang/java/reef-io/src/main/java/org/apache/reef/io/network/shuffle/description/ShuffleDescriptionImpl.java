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
package org.apache.reef.io.network.shuffle.description;

import org.apache.reef.io.network.shuffle.params.*;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.*;

/**
 *
 */
public final class ShuffleDescriptionImpl implements ShuffleDescription {

  private final Class<? extends Name<String>> shuffleName;
  private final Map<String, List<String>> senderIdListMap;
  private final Map<String, List<String>> receiverIdListMap;
  private final Map<String, GroupingDescription> groupingDescriptionMap;
  private final List<String> groupingNameList;


  @Inject
  public ShuffleDescriptionImpl(
      final @Parameter(SerializedShuffleName.class) String shuffleName,
      final @Parameter(SerializedGroupingSet.class) Set<String> serializedGroupings,
      final ConfigurationSerializer confSerializer) {
    try {
      this.shuffleName = (Class<? extends Name<String>>) Class.forName(shuffleName);
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    this.senderIdListMap = new HashMap<>();
    this.receiverIdListMap = new HashMap<>();
    this.groupingDescriptionMap = new HashMap<>();
    this.groupingNameList = new ArrayList<>();
    for (final String serializedGrouping : serializedGroupings) {
      deserializeGrouping(serializedGrouping, confSerializer);
    }
  }

  private void deserializeGrouping(final String serializedGrouping, final ConfigurationSerializer confSerializer) {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector(confSerializer.fromString(serializedGrouping));
      final GroupingDescription description = injector.getInstance(GroupingDescription.class);
      final Set<String> senderIdSet = injector.getNamedInstance(GroupingSenderIdSet.class);
      final Set<String> receiverIdSet = injector.getNamedInstance(GroupingReceiverIdSet.class);

      final String groupingName = description.getGroupingName();
      groupingDescriptionMap.put(groupingName, description);
      groupingNameList.add(groupingName);
      senderIdListMap.put(groupingName, getSortedListFromSet(senderIdSet));
      receiverIdListMap.put(groupingName, getSortedListFromSet(receiverIdSet));
    } catch (final Exception exception) {
      throw new RuntimeException("An Exception occurred while deserializing grouping " + serializedGrouping, exception);
    }
  }

  private List<String> getSortedListFromSet(final Set<String> set) {
    final List<String> list = new ArrayList<>(set);
    Collections.sort(list);
    return list;
  }

  private ShuffleDescriptionImpl(
      final Class<? extends Name<String>> shuffleName,
      final Map<String, List<String>> senderIdListMap,
      final Map<String, List<String>> receiverIdListMap,
      final Map<String, GroupingDescription> groupingDescriptionMap,
      final List<String> groupingNameList) {
    this.shuffleName = shuffleName;
    this.senderIdListMap = senderIdListMap;
    this.receiverIdListMap = receiverIdListMap;
    this.groupingDescriptionMap = groupingDescriptionMap;
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
  public GroupingDescription getGroupingDescription(final String groupingName) {
    return groupingDescriptionMap.get(groupingName);
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
    private final Map<String, GroupingDescription> groupingDescriptionMap;
    private final List<String> groupingNameList;

    private Builder(Class<? extends Name<String>> shuffleName) {
      this.shuffleName = shuffleName;
      this.senderIdListMap = new HashMap<>();
      this.receiverIdListMap = new HashMap<>();
      this.groupingDescriptionMap = new HashMap<>();
      this.groupingNameList = new ArrayList<>();
    }

    public Builder addGrouping(final List<String> senderIdList, final List<String> receiverIdList, final GroupingDescription groupingDescription) {
      if (groupingDescriptionMap.containsKey(groupingDescription.getGroupingName())) {
        throw new RuntimeException(groupingDescription.getGroupingName() + " was already added.");
      }

      groupingDescriptionMap.put(groupingDescription.getGroupingName(), groupingDescription);
      groupingNameList.add(groupingDescription.getGroupingName());
      senderIdListMap.put(groupingDescription.getGroupingName(), senderIdList);
      receiverIdListMap.put(groupingDescription.getGroupingName(), receiverIdList);
      return this;
    }

    public ShuffleDescriptionImpl build() {
      return new ShuffleDescriptionImpl(
          shuffleName,
          senderIdListMap,
          receiverIdListMap,
          groupingDescriptionMap,
          groupingNameList
      );
    }
  }
}
