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
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.List;

/**
 *
 */
public final class ShuffleDescriptionSerializer {

  private final ConfigurationSerializer confSerializer;

  @Inject
  public ShuffleDescriptionSerializer(
      final ConfigurationSerializer confSerializer) {
    this.confSerializer = confSerializer;
  }

  public Configuration getConfigurationHasTaskId(final ShuffleDescription shuffleDescription, final String taskId, final Configuration baseConf) {
    final JavaConfigurationBuilder confBuilder = getBaseConfigurationBuilder(shuffleDescription, baseConf);
    boolean isTaskIncludedSomeGrouping = false;

    for (final String groupingName : shuffleDescription.getGroupingNameList()) {
      final GroupingDescription groupingDescription = shuffleDescription.getGroupingDescription(groupingName);

      final List<String> senderIdList = shuffleDescription.getSenderIdList(groupingName);
      final List<String> receiverIdList = shuffleDescription.getReceiverIdList(groupingName);
      if (senderIdList.contains(taskId) || receiverIdList.contains(taskId)) {
        isTaskIncludedSomeGrouping = true;
        bindGroupingDescription(confBuilder, shuffleDescription, groupingDescription);
      }
    }

    if (!isTaskIncludedSomeGrouping) {
      return null;
    }

    return confBuilder.build();
  }

  public Configuration getConfigurationHasTaskId(final ShuffleDescription shuffleDescription, final String taskId) {
    return getConfigurationHasTaskId(shuffleDescription, taskId, null);
  }

  public Configuration getConfiguration(final ShuffleDescription shuffleDescription, final Configuration baseConf) {
    final JavaConfigurationBuilder confBuilder = getBaseConfigurationBuilder(shuffleDescription, baseConf);

    for (final String groupingName : shuffleDescription.getGroupingNameList()) {
      final GroupingDescription groupingDescription = shuffleDescription.getGroupingDescription(groupingName);
      bindGroupingDescription(confBuilder, shuffleDescription, groupingDescription);
    }

    return confBuilder.build();
  }

  public Configuration getConfiguration(final ShuffleDescription shuffleDescription) {
    return getConfiguration(shuffleDescription, null);
  }

  private JavaConfigurationBuilder getBaseConfigurationBuilder(
      final ShuffleDescription shuffleDescription,
      final Configuration baseConf) {

    final JavaConfigurationBuilder confBuilder;
    if (baseConf != null) {
      confBuilder = Tang.Factory.getTang().newConfigurationBuilder(baseConf);
    } else {
      confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    }

    confBuilder.bindNamedParameter(SerializedShuffleName.class, shuffleDescription.getShuffleName().getName());
    return confBuilder;
  }

  private void bindGroupingDescription(
      final JavaConfigurationBuilder confBuilder,
      final ShuffleDescription shuffleDescription,
      final GroupingDescription groupingDescription) {
    final Configuration groupingConfiguration = serializeGroupingDescriptionWithSenderReceiver(
        shuffleDescription, groupingDescription);

    confBuilder.bindSetEntry(SerializedGroupingSet.class, confSerializer.toString(groupingConfiguration));
  }

  private Configuration serializeGroupingDescriptionWithSenderReceiver(
      final ShuffleDescription shuffleDescription, final GroupingDescription groupingDescription) {

    final String groupingName = groupingDescription.getGroupingName();

    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    confBuilder.bindNamedParameter(GroupingName.class, groupingName);
    confBuilder.bindNamedParameter(GroupingStrategyClassName.class, groupingDescription.getGroupingStrategyClass().getName());
    confBuilder.bindNamedParameter(GroupingKeyCodecClassName.class, groupingDescription.getKeyCodec().getName());
    confBuilder.bindNamedParameter(GroupingValueCodecClassName.class, groupingDescription.getValueCodec().getName());

    for (final String senderId : shuffleDescription.getSenderIdList(groupingName)) {
      confBuilder.bindSetEntry(GroupingSenderIdSet.class, senderId);
    }

    for (final String receiverId : shuffleDescription.getReceiverIdList(groupingName)) {
      confBuilder.bindSetEntry(GroupingReceiverIdSet.class, receiverId);
    }

    return confBuilder.build();
  }
}
