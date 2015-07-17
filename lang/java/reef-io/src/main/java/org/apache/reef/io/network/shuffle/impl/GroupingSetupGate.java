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

import org.apache.reef.io.network.shuffle.driver.ShuffleManager;
import org.apache.reef.io.network.shuffle.network.ShuffleControlMessage;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
final class GroupingSetupGate {

  private boolean isOpened;

  private ShuffleManager shuffleManager;
  private String groupingName;
  private final Map<String, Boolean> taskStartedMap;
  private int startedTaskNum;

  GroupingSetupGate(
      final ShuffleManager shuffleManager,
      final String groupingName,
      final Set<String> taskIdSet) {

    this.shuffleManager = shuffleManager;
    this.groupingName = groupingName;
    this.taskStartedMap = new ConcurrentHashMap<>(taskIdSet.size());
    for (final String taskId : taskIdSet) {
      taskStartedMap.put(taskId, false);
    }
  }

  synchronized boolean isOpened() {
    return isOpened;
  }

  void onTaskStarted(final String taskId) {
    if (!taskStartedMap.containsKey(taskId)) {
      return;
    }

    if (isOpened) {
      sendGroupingSetupMessage(taskId);
      return;
    }

    boolean firstOpened = false;

    synchronized (this) {
      if (!taskStartedMap.get(taskId)) {
        taskStartedMap.put(taskId, true);
        startedTaskNum++;
        if (startedTaskNum == taskStartedMap.size()) {
          isOpened = true;
          firstOpened = true;
        }
      }
    }

    if (firstOpened) {
      broadcastGroupingSetupMessage();
    }
  }

  void onTaskStopped(final String taskId) {
    if (!taskStartedMap.containsKey(taskId)) {
      return;
    }

    synchronized (this) {
      if (taskStartedMap.get(taskId)) {
        taskStartedMap.put(taskId, false);
        startedTaskNum--;
      }
    }
  }

  private void broadcastGroupingSetupMessage() {
    for (final String taskId : taskStartedMap.keySet()) {
      if (taskStartedMap.get(taskId)) {
        sendGroupingSetupMessage(taskId);
      }
    }
  }

  private void sendGroupingSetupMessage(final String taskId) {
    shuffleManager.sendControlMessage(
        taskId, StaticShuffleMessageCode.GROUPING_SETUP, groupingName, null, ShuffleControlMessage.MANAGER, ShuffleControlMessage.CLIENT);
  }
}
