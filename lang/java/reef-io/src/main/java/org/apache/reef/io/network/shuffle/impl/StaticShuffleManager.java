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
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.shuffle.GroupingController;
import org.apache.reef.io.network.shuffle.driver.ShuffleDriver;
import org.apache.reef.io.network.shuffle.driver.ShuffleManager;
import org.apache.reef.io.network.shuffle.network.ShuffleControlMessage;
import org.apache.reef.io.network.shuffle.task.ShuffleClient;
import org.apache.reef.io.network.shuffle.description.GroupingDescription;
import org.apache.reef.io.network.shuffle.description.ShuffleDescription;
import org.apache.reef.io.network.shuffle.description.ShuffleDescriptionSerializer;
import org.apache.reef.tang.Configuration;

import javax.inject.Inject;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 *
 */
public final class StaticShuffleManager implements ShuffleManager {

  private static final Logger LOG = Logger.getLogger(StaticShuffleManager.class.getName());

  private final ShuffleDriver shuffleDriver;

  private final ShuffleDescription initialShuffleDescription;
  private final String shuffleName;
  private final ShuffleDescriptionSerializer shuffleDescriptionSerializer;
  private final Map<String, GroupingSetupGate> groupingSetupGates;

  @Inject
  public StaticShuffleManager(
      final ShuffleDriver shuffleDriver,
      final ShuffleDescription initialShuffleDescription,
      final ShuffleDescriptionSerializer shuffleDescriptionSerializer) {
    this.shuffleDriver = shuffleDriver;
    this.initialShuffleDescription = initialShuffleDescription;
    this.shuffleName = initialShuffleDescription.getShuffleName().getName();
    this.shuffleDescriptionSerializer = shuffleDescriptionSerializer;
    this.groupingSetupGates = new ConcurrentHashMap<>();
    createGroupingSetupGates();
  }

  private void createGroupingSetupGates() {
    for (final String groupingName : initialShuffleDescription.getGroupingNameList()) {
      final GroupingDescription description = initialShuffleDescription.getGroupingDescription(groupingName);
      final Set<String> taskIdSet = new HashSet<>();
      for (final String senderId : initialShuffleDescription.getSenderIdList(description.getGroupingName())) {
        taskIdSet.add(senderId);
      }

      for (final String receiverId : initialShuffleDescription.getReceiverIdList(description.getGroupingName())) {
        taskIdSet.add(receiverId);
      }

      groupingSetupGates.put(
          groupingName,
          new GroupingSetupGate(
              this,
              groupingName,
              taskIdSet
          )
      );
    }
  }

  @Override
  public ShuffleDescription getShuffleDescription() {
    return initialShuffleDescription;
  }

  @Override
  public Configuration getShuffleDescriptionConfigurationForTask(final String taskId) {
    return shuffleDescriptionSerializer.getConfigurationHasTaskId(initialShuffleDescription, taskId);
  }

  @Override
  public Class<? extends ShuffleClient> getClientClass() {
    return StaticShuffleClient.class;
  }

  @Override
  public void onRunningTask(final RunningTask runningTask) {
    for (final GroupingSetupGate gate : groupingSetupGates.values()) {
      gate.onTaskStarted(runningTask.getId());
    }
  }

  @Override
  public void onFailedTask(final FailedTask failedTask) {
    onTaskStopped(failedTask.getId());
  }

  @Override
  public void onCompletedTask(final CompletedTask completedTask) {
    onTaskStopped(completedTask.getId());
  }

  @Override
  public void registerGroupingController(GroupingController groupingController) {
    shuffleDriver.registerGroupingController(shuffleName, groupingController);
  }

  @Override
  public void sendControlMessage(String destId, int code, String groupingName, byte[][] data, byte sourceType, byte sinkType) {
    shuffleDriver.sendControlMessage(destId, code, shuffleName, groupingName, data, sourceType, sinkType);
  }

  private void onTaskStopped(final String taskId) {
    for (final GroupingSetupGate gate : groupingSetupGates.values()) {
      gate.onTaskStopped(taskId);
    }
  }

  @Override
  public void onNext(Message<ShuffleControlMessage> value) {
  }

  @Override
  public void onSuccess(Message<ShuffleControlMessage> message) {
  }

  @Override
  public void onException(Throwable cause, SocketAddress remoteAddress, Message<ShuffleControlMessage> message) {
  }
}
