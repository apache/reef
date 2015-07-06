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

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.shuffle.ns.ShuffleControlMessage;
import org.apache.reef.io.network.shuffle.topology.TopologyDescription;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */

final class TaskEntityMap {

  private final TopologyDescription topologyDescription;
  private final IdentifierFactory idFactory;
  private final ConnectionFactory<ShuffleControlMessage> connectionFactory;
  private final Map<String, TaskEntity> entityMap;

  private boolean topologyInitailized;
  private int waitingTaskNum;

  TaskEntityMap(
      final TopologyDescription topologyDescription,
      final IdentifierFactory idFactory,
      final ConnectionFactory<ShuffleControlMessage> connectionFactory) {
    this.topologyDescription = topologyDescription;
    this.idFactory = idFactory;
    this.connectionFactory = connectionFactory;
    this.entityMap = new HashMap<>();
  }

  void putTaskIdIfAbsent(final String taskId) {
    if (!entityMap.containsKey(taskId)) {
      waitingTaskNum++;
      entityMap.put(taskId, new TaskEntity(taskId));
    }
  }

  synchronized void onTaskStart(final String taskId) {
    final TaskState currentState = entityMap.get(taskId).getState();
    if (currentState == TaskState.RUNNING) {
      throw new RuntimeException("The onTask should not be called with RUNNING task id");
    } else {
      entityMap.get(taskId).setState(TaskState.RUNNING);
      waitingTaskNum--;
    }

    if (!topologyInitailized && waitingTaskNum == 0) {
      topologyInitailized = true;
      sendTopologySetupMessages();
    }
  }

  synchronized void onTaskStop(final String taskId) {
    final TaskState currentState = entityMap.get(taskId).getState();
    if (currentState == TaskState.RUNNING) {
      entityMap.get(taskId).setState(TaskState.WAITING);
      waitingTaskNum++;
    }
  }

  private void sendTopologySetupMessages() {
    final ShuffleControlMessage controlMessage = new ShuffleControlMessage(
        ImmutableShuffleMessageCode.TOPOLOGY_SETUP,
        topologyDescription.getTopologyName().getName(),
        null,
        null);

    for (final TaskEntity entity : entityMap.values()) {
      entity.sendMessage(controlMessage);
    }
  }

  enum TaskState {
    WAITING,
    RUNNING,
  }

  final class TaskEntity {

    private final Identifier taskId;
    private TaskState state;
    private Connection<ShuffleControlMessage> connection;

    TaskEntity(final String taskId) {
      this.taskId = idFactory.getNewInstance(taskId);
      this.state = TaskState.WAITING;
      this.connection = connectionFactory.newConnection(this.taskId);
    }

    Identifier getTaskId() {
      return taskId;
    }

    TaskState getState() {
      return state;
    }

    void setState(final TaskState state) {
      this.state = state;
    }

    void sendMessage(final ShuffleControlMessage message) {
      try {
        this.connection.open();
        this.connection.write(message);
      } catch (NetworkException e) {
        // unnecessary try-catch clause.
      }
    }
  }
}
