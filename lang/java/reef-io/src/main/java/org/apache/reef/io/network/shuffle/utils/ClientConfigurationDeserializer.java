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

import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.io.network.shuffle.params.*;
import org.apache.reef.io.network.shuffle.task.*;
import org.apache.reef.io.network.shuffle.topology.GroupingDescriptor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.*;

/**
 *
 */
public final class ClientConfigurationDeserializer {

  private final Set<String> serializedGroupingSet;
  private final ConfigurationSerializer confSerializer;

  private final Map<String, GroupingDescriptor> groupingDescriptorMap;
  private final Map<String, List<String>> senderIdListMap;
  private final Map<String, List<String>> receiverIdListMap;
  private final Map<String, StreamingCodec<Tuple>> tupleCodecMap;
  private final List<String> groupingNameList;

  @Inject
  public ClientConfigurationDeserializer(
      final @Parameter(SerializedGroupingSet.class) Set<String> serializedGroupingSet,
      final ConfigurationSerializer confSerializer) {

    this.serializedGroupingSet = serializedGroupingSet;
    this.confSerializer = confSerializer;

    this.groupingDescriptorMap = new HashMap<>();
    this.senderIdListMap = new HashMap<>();
    this.receiverIdListMap = new HashMap<>();
    this.groupingNameList = new ArrayList<>();
    this.tupleCodecMap = new HashMap<>();

    deserializeGroupingSet();
  }

  private void deserializeGroupingSet() {
    for (final String serializedGrouping : serializedGroupingSet) {
      deserializeGrouping(serializedGrouping);
    }
  }

  private void deserializeGrouping(final String serializedGrouping) {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector(confSerializer.fromString(serializedGrouping));
      final GroupingDescriptor descriptor = injector.getInstance(GroupingDescriptor.class);
      final Set<String> senderIdSet = injector.getNamedInstance(GroupingSenderIdSet.class);
      final Set<String> receiverIdSet = injector.getNamedInstance(GroupingReceiverIdSet.class);

      final String groupingName = descriptor.getGroupingName();
      groupingDescriptorMap.put(groupingName, descriptor);
      groupingNameList.add(groupingName);
      senderIdListMap.put(groupingName, getSortedListFromSet(senderIdSet));
      receiverIdListMap.put(groupingName, getSortedListFromSet(receiverIdSet));
      createTupleCodec(descriptor);
    } catch (final Exception exception) {
      throw new RuntimeException("An Exception occurred while deserializing grouping " + serializedGrouping, exception);
    }
  }

  private List<String> getSortedListFromSet(final Set<String> set) {
    final List<String> list = new ArrayList<>(set);
    Collections.sort(list);
    return list;
  }

  private void createTupleCodec(GroupingDescriptor groupingDescriptor) {
    final Configuration codecConfiguration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(ShuffleKeyCodec.class, groupingDescriptor.getKeyCodec())
        .bindNamedParameter(ShuffleValueCodec.class, groupingDescriptor.getValueCodec())
        .build();

    try {
      tupleCodecMap.put(
          groupingDescriptor.getGroupingName(),
          Tang.Factory.getTang().newInjector(codecConfiguration)
              .getInstance(TupleCodec.class)
      );
    } catch (InjectionException e) {
      throw new RuntimeException("An InjectionException occurred while injecting tuple codec with " + groupingDescriptor, e);
    }
  }

  public Map<String, GroupingDescriptor> getGroupingDescriptorMap() {
    return groupingDescriptorMap;
  }

  public Map<String, List<String>> getSenderIdListMap() {
    return senderIdListMap;
  }

  public Map<String, List<String>> getReceiverIdListMap() {
    return receiverIdListMap;
  }

  public List<String> getGroupingNameList() {
    return groupingNameList;
  }

  public Map<String, StreamingCodec<Tuple>> getTupleCodecMap() {
    return tupleCodecMap;
  }
}
