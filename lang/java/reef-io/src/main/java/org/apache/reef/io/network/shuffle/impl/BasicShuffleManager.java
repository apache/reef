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
package org.apache.reef.io.network.shuffle.impl;

import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.io.network.shuffle.description.GroupingDescription;
import org.apache.reef.io.network.shuffle.driver.ShuffleManager;
import org.apache.reef.io.network.shuffle.params.GroupingParameters;
import org.apache.reef.io.network.shuffle.params.ShuffleParameters;
import org.apache.reef.io.network.shuffle.task.ShuffleClient;
import org.apache.reef.io.network.shuffle.description.ShuffleDescription;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.util.List;

/**
 *
 */
public final class BasicShuffleManager implements ShuffleManager {

  private final ShuffleDescription initialShuffleDescription;
  private final ConfigurationSerializer confSerializer;

  @Inject
  public BasicShuffleManager(
      final ShuffleDescription initialShuffleDescription,
      final ConfigurationSerializer confSerializer) {
    this.initialShuffleDescription = initialShuffleDescription;
    this.confSerializer = confSerializer;
  }

  private void bindGroupingDescription(
      final JavaConfigurationBuilder confBuilder,
      final ShuffleDescription shuffleDescription,
      final GroupingDescription groupingDescription) {
    final Configuration groupingConfiguration = serializeGroupingDescriptionWithSenderReceiver(
        shuffleDescription, groupingDescription);

    confBuilder.bindSetEntry(
        ShuffleParameters.SerializedGroupingSet.class, confSerializer.toString(groupingConfiguration));
  }

  private Configuration serializeGroupingDescriptionWithSenderReceiver(
      final ShuffleDescription shuffleDescription, final GroupingDescription groupingDescription) {

    final String groupingName = groupingDescription.getGroupingName();

    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    confBuilder.bindNamedParameter(GroupingParameters.GroupingName.class, groupingName);
    confBuilder.bindNamedParameter(
        GroupingParameters.GroupingStrategyClassName.class, groupingDescription.getGroupingStrategyClass().getName());
    confBuilder.bindNamedParameter(
        GroupingParameters.GroupingKeyCodecClassName.class, groupingDescription.getKeyCodecClass().getName());
    confBuilder.bindNamedParameter(
        GroupingParameters.GroupingValueCodecClassName.class, groupingDescription.getValueCodecClass().getName());

    for (final String senderId : shuffleDescription.getSenderIdList(groupingName)) {
      confBuilder.bindSetEntry(GroupingParameters.GroupingSenderIdSet.class, senderId);
    }

    for (final String receiverId : shuffleDescription.getReceiverIdList(groupingName)) {
      confBuilder.bindSetEntry(GroupingParameters.GroupingReceiverIdSet.class, receiverId);
    }

    return confBuilder.build();
  }

  @Override
  public Configuration getShuffleDescriptionConfigurationForTask(final String taskId) {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    confBuilder.bindNamedParameter(ShuffleParameters.SerializedShuffleName.class,
        initialShuffleDescription.getShuffleName());

    boolean isTaskIncludedSomeGrouping = false;

    System.out.println(taskId + " " + initialShuffleDescription.getGroupingNameList());
    for (final String groupingName : initialShuffleDescription.getGroupingNameList()) {

      final GroupingDescription groupingDescription = initialShuffleDescription.getGroupingDescription(groupingName);

      final List<String> senderIdList = initialShuffleDescription.getSenderIdList(groupingName);
      final List<String> receiverIdList = initialShuffleDescription.getReceiverIdList(groupingName);
      if (senderIdList.contains(taskId) || receiverIdList.contains(taskId)) {
        isTaskIncludedSomeGrouping = true;
        bindGroupingDescription(confBuilder, initialShuffleDescription, groupingDescription);
      }

      System.out.println(taskId + " " + initialShuffleDescription.getGroupingNameList());
      System.out.println(senderIdList + " " + receiverIdList);
    }

    if (!isTaskIncludedSomeGrouping) {
      return null;
    }

    return confBuilder.build();
  }

  @Override
  public Class<? extends ShuffleClient> getClientClass() {
    return BasicShuffleClient.class;
  }

  @Override
  public void onRunningTask(final RunningTask runningTask) {
  }

  @Override
  public void onFailedTask(final FailedTask failedTask) {
  }

  @Override
  public void onCompletedTask(final CompletedTask completedTask) {
  }
}
