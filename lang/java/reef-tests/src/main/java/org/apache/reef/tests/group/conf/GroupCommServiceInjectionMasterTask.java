/*
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
package org.apache.reef.tests.group.conf;

import org.apache.reef.io.network.group.api.operators.Broadcast;
import org.apache.reef.io.network.group.api.task.CommunicationGroupClient;
import org.apache.reef.io.network.group.api.task.GroupCommClient;
import org.apache.reef.task.Task;
import org.apache.reef.tests.group.conf.GroupCommServiceInjectionDriver.GroupCommServiceInjectionBroadcast;
import org.apache.reef.tests.group.conf.GroupCommServiceInjectionDriver.GroupCommServiceInjectionGroupName;

import javax.inject.Inject;

/**
 * Master task used for the GroupCommServiceInjection test.
 * Sends a single integer to the {@link GroupCommServiceInjectionSlaveTask}.
 */
final class GroupCommServiceInjectionMasterTask implements Task {
  static final String TASK_ID = "GroupCommServiceInjectionMasterTask";

  private final Broadcast.Sender<Integer> sender;

  @Inject
  private GroupCommServiceInjectionMasterTask(final GroupCommClient groupCommClient) {
    final CommunicationGroupClient commGroupClient =
        groupCommClient.getCommunicationGroup(GroupCommServiceInjectionGroupName.class);
    this.sender = commGroupClient.getBroadcastSender(GroupCommServiceInjectionBroadcast.class);
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    sender.send(GroupCommServiceInjectionDriver.SEND_INTEGER);
    return null;
  }
}
