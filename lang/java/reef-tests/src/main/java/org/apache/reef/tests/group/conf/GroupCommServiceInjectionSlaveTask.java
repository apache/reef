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
 * Slave task used for the GroupCommServiceInjection test.
 * Receives a single integer from {@link GroupCommServiceInjectionMasterTask} and checks that
 * {@link GroupCommServiceInjectionCodec} adds the correct offset to the received integer.
 */
final class GroupCommServiceInjectionSlaveTask implements Task {
  static final String TASK_ID = "GroupCommServiceInjectionSlaveTask";

  private final Broadcast.Receiver<Integer> receiver;

  @Inject
  private GroupCommServiceInjectionSlaveTask(final GroupCommClient groupCommClient) {
    final CommunicationGroupClient commGroupClient =
        groupCommClient.getCommunicationGroup(GroupCommServiceInjectionGroupName.class);
    this.receiver = commGroupClient.getBroadcastReceiver(GroupCommServiceInjectionBroadcast.class);
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    final int expected = GroupCommServiceInjectionDriver.SEND_INTEGER + GroupCommServiceInjectionDriver.OFFSET;
    final int received = receiver.receive();
    if (received != expected) {
      throw new RuntimeException(String.format("Expected %d but received %d", expected, received));
    }

    return null;
  }
}
