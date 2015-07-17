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

import org.apache.reef.io.network.shuffle.description.ShuffleDescription;
import org.apache.reef.io.network.shuffle.driver.ShuffleManager;
import org.apache.reef.io.network.shuffle.params.ShuffleParameters;
import org.apache.reef.io.network.shuffle.task.ShuffleClient;
import org.apache.reef.io.network.shuffle.description.ShuffleGroupDescription;
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

  private final ShuffleGroupDescription initialShuffleGroupDescription;
  private final ConfigurationSerializer confSerializer;

  @Inject
  public BasicShuffleManager(
      final ShuffleGroupDescription initialShuffleGroupDescription,
      final ConfigurationSerializer confSerializer) {
    this.initialShuffleGroupDescription = initialShuffleGroupDescription;
    this.confSerializer = confSerializer;
  }

  @Override
  public Configuration getClientConfigurationForTask(final String taskId) {
    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    confBuilder.bindNamedParameter(ShuffleParameters.SerializedShuffleGroupName.class,
        initialShuffleGroupDescription.getShuffleGroupName());

    boolean isTaskIncludedSomeShuffle = false;

    for (final String shuffleName : initialShuffleGroupDescription.getShuffleNameList()) {
      final ShuffleDescription shuffleDescription = initialShuffleGroupDescription.getShuffleDescription(shuffleName);
      final List<String> senderIdList = initialShuffleGroupDescription.getSenderIdList(shuffleName);
      final List<String> receiverIdList = initialShuffleGroupDescription.getReceiverIdList(shuffleName);
      if (senderIdList.contains(taskId) || receiverIdList.contains(taskId)) {
        isTaskIncludedSomeShuffle = true;
        bindShuffleDescription(confBuilder, shuffleDescription);
      }
    }

    if (!isTaskIncludedSomeShuffle) {
      return null;
    }
    return confBuilder.build();
  }

  private void bindShuffleDescription(
      final JavaConfigurationBuilder confBuilder,
      final ShuffleDescription shuffleDescription) {
    final Configuration shuffleConfiguration = serializeShuffleDescriptionWithSenderReceiver(shuffleDescription);

    confBuilder.bindSetEntry(
        ShuffleParameters.SerializedShuffleSet.class, confSerializer.toString(shuffleConfiguration));
  }

  private Configuration serializeShuffleDescriptionWithSenderReceiver(final ShuffleDescription shuffleDescription) {

    final String shuffleName = shuffleDescription.getShuffleName();

    final JavaConfigurationBuilder confBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    confBuilder.bindNamedParameter(ShuffleParameters.ShuffleName.class, shuffleName);
    confBuilder.bindNamedParameter(
        ShuffleParameters.ShuffleStrategyClassName.class, shuffleDescription.getShuffleStrategyClass().getName());
    confBuilder.bindNamedParameter(
        ShuffleParameters.ShuffleKeyCodecClassName.class, shuffleDescription.getKeyCodecClass().getName());
    confBuilder.bindNamedParameter(
        ShuffleParameters.ShuffleValueCodecClassName.class, shuffleDescription.getValueCodecClass().getName());

    for (final String senderId : initialShuffleGroupDescription.getSenderIdList(shuffleName)) {
      confBuilder.bindSetEntry(ShuffleParameters.ShuffleSenderIdSet.class, senderId);
    }

    for (final String receiverId : initialShuffleGroupDescription.getReceiverIdList(shuffleName)) {
      confBuilder.bindSetEntry(ShuffleParameters.ShuffleReceiverIdSet.class, receiverId);
    }

    return confBuilder.build();
  }

  @Override
  public Class<? extends ShuffleClient> getClientClass() {
    return BasicShuffleClient.class;
  }

}
