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
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.*;

/**
 *
 */
public final class ShuffleGroupDescriptionImpl implements ShuffleGroupDescription {

  private final String shuffleGroupName;
  private final Map<String, List<String>> senderIdListMap;
  private final Map<String, List<String>> receiverIdListMap;
  private final Map<String, ShuffleDescription> shuffleDescriptionMap;
  private final List<String> shuffleNameList;

  @Inject
  public ShuffleGroupDescriptionImpl(
      @Parameter(ShuffleParameters.SerializedShuffleGroupName.class) final String shuffleGroupName,
      @Parameter(ShuffleParameters.SerializedShuffleSet.class) final Set<String> serializedShuffleSet,
      final ConfigurationSerializer confSerializer) {
    this.shuffleGroupName = shuffleGroupName;
    this.senderIdListMap = new HashMap<>();
    this.receiverIdListMap = new HashMap<>();
    this.shuffleDescriptionMap = new HashMap<>();
    this.shuffleNameList = new ArrayList<>();
    for (final String serializedShuffle : serializedShuffleSet) {
      deserializeShuffle(serializedShuffle, confSerializer);
    }
  }

  private void deserializeShuffle(final String serializedShuffle, final ConfigurationSerializer confSerializer) {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector(confSerializer.fromString(serializedShuffle));
      final ShuffleDescription description = injector.getInstance(ShuffleDescription.class);
      final Set<String> senderIdSet = injector.getNamedInstance(ShuffleParameters.ShuffleSenderIdSet.class);
      final Set<String> receiverIdSet = injector.getNamedInstance(ShuffleParameters.ShuffleReceiverIdSet.class);

      final String shuffleName = description.getShuffleName();
      shuffleDescriptionMap.put(shuffleName, description);
      shuffleNameList.add(shuffleName);
      senderIdListMap.put(shuffleName, getSortedListFromSet(senderIdSet));
      receiverIdListMap.put(shuffleName, getSortedListFromSet(receiverIdSet));
    } catch (final Exception exception) {
      throw new RuntimeException("An Exception occurred while deserializing shuffle " + serializedShuffle, exception);
    }
  }

  private List<String> getSortedListFromSet(final Set<String> set) {
    final List<String> list = new ArrayList<>(set);
    Collections.sort(list);
    return list;
  }

  private ShuffleGroupDescriptionImpl(
      final String shuffleGroupName,
      final Map<String, List<String>> senderIdListMap,
      final Map<String, List<String>> receiverIdListMap,
      final Map<String, ShuffleDescription> shuffleDescriptionMap,
      final List<String> shuffleNameList) {
    this.shuffleGroupName = shuffleGroupName;
    this.senderIdListMap = senderIdListMap;
    this.receiverIdListMap = receiverIdListMap;
    this.shuffleDescriptionMap = shuffleDescriptionMap;
    this.shuffleNameList = shuffleNameList;
  }

  @Override
  public String getShuffleGroupName() {
    return shuffleGroupName;
  }

  @Override
  public List<String> getShuffleNameList() {
    return shuffleNameList;
  }

  @Override
  public ShuffleDescription getShuffleDescription(final String shuffleName) {
    return shuffleDescriptionMap.get(shuffleName);
  }

  @Override
  public List<String> getSenderIdList(final String shuffleName) {
    return senderIdListMap.get(shuffleName);
  }

  @Override
  public List<String> getReceiverIdList(final String shuffleName) {
    return receiverIdListMap.get(shuffleName);
  }

  public static Builder newBuilder(final String shuffleGroupName) {
    return new Builder(shuffleGroupName);
  }

  public static final class Builder {

    private final String shuffleGroupName;
    private final Map<String, List<String>> senderIdListMap;
    private final Map<String, List<String>> receiverIdListMap;
    private final Map<String, ShuffleDescription> shuffleDescriptionMap;
    private final List<String> shuffleNameList;

    private Builder(final String shuffleGroupName) {
      this.shuffleGroupName = shuffleGroupName;
      this.senderIdListMap = new HashMap<>();
      this.receiverIdListMap = new HashMap<>();
      this.shuffleDescriptionMap = new HashMap<>();
      this.shuffleNameList = new ArrayList<>();
    }

    public Builder addShuffle(
        final List<String> senderIdList,
        final List<String> receiverIdList,
        final ShuffleDescription shuffleDescription) {
      if (shuffleDescriptionMap.containsKey(shuffleDescription.getShuffleName())) {
        throw new RuntimeException(shuffleDescription.getShuffleName() + " was already added.");
      }

      shuffleDescriptionMap.put(shuffleDescription.getShuffleName(), shuffleDescription);
      shuffleNameList.add(shuffleDescription.getShuffleName());
      senderIdListMap.put(shuffleDescription.getShuffleName(), senderIdList);
      receiverIdListMap.put(shuffleDescription.getShuffleName(), receiverIdList);
      return this;
    }

    public ShuffleGroupDescriptionImpl build() {
      return new ShuffleGroupDescriptionImpl(
          shuffleGroupName,
          senderIdListMap,
          receiverIdListMap,
          shuffleDescriptionMap,
          shuffleNameList
      );
    }
  }
}
