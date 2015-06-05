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
package org.apache.reef.io.network.group.impl.driver;

import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.utils.BroadcastingEventHandler;
import org.apache.reef.io.network.group.impl.utils.Utils;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.wake.EventHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * The network handler for the group communcation service on the driver side.
 */
public class GroupCommMessageHandler implements EventHandler<GroupCommunicationMessage> {

  private static final Logger LOG = Logger.getLogger(GroupCommMessageHandler.class.getName());

  private final Map<Class<? extends Name<String>>, BroadcastingEventHandler<GroupCommunicationMessage>>
      commGroupMessageHandlers = new HashMap<>();

  public void addHandler(final Class<? extends Name<String>> groupName,
                         final BroadcastingEventHandler<GroupCommunicationMessage> handler) {
    LOG.entering("GroupCommMessageHandler", "addHandler", new Object[]{Utils.simpleName(groupName), handler});
    commGroupMessageHandlers.put(groupName, handler);
    LOG.exiting("GroupCommMessageHandler", "addHandler", Utils.simpleName(groupName));
  }

  @Override
  public void onNext(final GroupCommunicationMessage msg) {
    LOG.entering("GroupCommMessageHandler", "onNext", msg);
    final Class<? extends Name<String>> groupName = Utils.getClass(msg.getGroupname());
    commGroupMessageHandlers.get(groupName).onNext(msg);
    LOG.exiting("GroupCommMessageHandler", "onNext", msg);
  }
}
