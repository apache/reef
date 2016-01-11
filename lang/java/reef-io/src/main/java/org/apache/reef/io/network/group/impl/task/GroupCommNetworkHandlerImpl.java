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
package org.apache.reef.io.network.group.impl.task;

import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.group.api.task.GroupCommNetworkHandler;
import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class GroupCommNetworkHandlerImpl implements GroupCommNetworkHandler {

  private static final Logger LOG = Logger.getLogger(GroupCommNetworkHandlerImpl.class.getName());

  private final Map<Class<? extends Name<String>>, EventHandler<GroupCommunicationMessage>> commGroupHandlers =
      new ConcurrentHashMap<>();

  @Inject
  public GroupCommNetworkHandlerImpl() {
  }

  @Override
  public void onNext(final Message<GroupCommunicationMessage> mesg) {
    LOG.entering("GroupCommNetworkHandlerImpl", "onNext", mesg);
    final Iterator<GroupCommunicationMessage> iter = mesg.getData().iterator();
    final GroupCommunicationMessage msg = iter.hasNext() ? iter.next() : null;
    if (msg != null) {
      try {
        final Class<? extends Name<String>> groupName =
            (Class<? extends Name<String>>) Class.forName(msg.getGroupname());
        commGroupHandlers.get(groupName).onNext(msg);
      } catch (final ClassNotFoundException e) {
        throw new RuntimeException("GroupName not found", e);
      }
    }
    LOG.exiting("GroupCommNetworkHandlerImpl", "onNext", mesg);
  }

  @Override
  public void register(final Class<? extends Name<String>> groupName,
                       final EventHandler<GroupCommunicationMessage> commGroupNetworkHandler) {
    LOG.entering("GroupCommNetworkHandlerImpl", "register", new Object[]{groupName,
        commGroupNetworkHandler});
    commGroupHandlers.put(groupName, commGroupNetworkHandler);
    LOG.exiting("GroupCommNetworkHandlerImpl", "register", Arrays.toString(new Object[]{groupName,
        commGroupNetworkHandler}));
  }

}
