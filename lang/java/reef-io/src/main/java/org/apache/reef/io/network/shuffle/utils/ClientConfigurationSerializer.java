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
import org.apache.reef.io.network.shuffle.task.ShuffleClient;
import org.apache.reef.io.network.shuffle.topology.GroupingDescription;
import org.apache.reef.io.network.shuffle.topology.GroupingDescriptor;
import org.apache.reef.io.network.shuffle.topology.ShuffleDescriptor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.List;

/**
 *
 */
public final class ClientConfigurationSerializer {

  private final ConfigurationSerializer confSerializer;

  @Inject
  public ClientConfigurationSerializer(
      final ConfigurationSerializer confSerializer) {
    this.confSerializer = confSerializer;
  }

  public Configuration serialize(final String taskId, final ShuffleDescriptor shuffleDescriptor,
                                 Class<? extends ShuffleClient> clientClass) {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    confBuilder.bindNamedParameter(SerializedShuffleName.class, shuffleDescriptor.getShuffleName().getName());
    confBuilder.bindImplementation(ShuffleClient.class, clientClass);

    boolean isTaskIncludedSomeGrouping = false;

    for (final String groupingName : shuffleDescriptor.getGroupingNameList()) {
      final GroupingDescriptor groupingDescriptor = shuffleDescriptor.getGroupingDescriptor(groupingName);

      final List<String> senderIdList = shuffleDescriptor.getSenderIdList(groupingName);
      final List<String> receiverIdList = shuffleDescriptor.getReceiverIdList(groupingName);
      if (senderIdList.contains(taskId) || receiverIdList.contains(taskId)) {
        isTaskIncludedSomeGrouping = true;
        confBuilder.bindSetEntry(SerializedGroupingSet.class,
            confSerializer.toString(serializeGroupingDescriptorWithSenderReceiver(shuffleDescriptor, groupingDescriptor)));
      }
    }

    if (!isTaskIncludedSomeGrouping) {
      return null;
    }

    return confBuilder.build();
  }

  private Configuration serializeGroupingDescriptorWithSenderReceiver(
      final ShuffleDescriptor shuffleDescriptor, final GroupingDescriptor groupingDescriptor) {

    final String groupingName = groupingDescriptor.getGroupingName();

    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    confBuilder.bindImplementation(GroupingDescriptor.class, GroupingDescription.class);
    confBuilder.bindNamedParameter(GroupingName.class, groupingName);
    confBuilder.bindNamedParameter(GroupingStrategyClassName.class, groupingDescriptor.getGroupingStrategyClass().getName());
    confBuilder.bindNamedParameter(GroupingKeyCodecClassName.class, groupingDescriptor.getKeyCodec().getName());
    confBuilder.bindNamedParameter(GroupingValueCodecClassName.class, groupingDescriptor.getValueCodec().getName());

    for (final String senderId : shuffleDescriptor.getSenderIdList(groupingName)) {
      confBuilder.bindSetEntry(GroupingSenderIdSet.class, senderId);
    }

    for (final String receiverId : shuffleDescriptor.getReceiverIdList(groupingName)) {
      confBuilder.bindSetEntry(GroupingReceiverIdSet.class, receiverId);
    }

    return confBuilder.build();
  }
}
