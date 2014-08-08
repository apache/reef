/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.reef.io.network.nggroup.impl;

import com.microsoft.reef.io.network.Message;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.tang.annotations.Name;
import com.microsoft.wake.EventHandler;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class GroupCommNetworkHandlerImpl implements com.microsoft.reef.io.network.nggroup.api.GroupCommNetworkHandler {
  private final Map<Class<? extends Name<String>>, EventHandler<GroupCommMessage>> commGroupHandlers = new ConcurrentHashMap<>();

  @Inject
  public GroupCommNetworkHandlerImpl() {
  }

  @Override
  public void onNext(Message<GroupCommMessage> mesg) {
    final Iterator<GroupCommMessage> iter = mesg.getData().iterator();
    GroupCommMessage msg = iter.hasNext() ? iter.next() : null;
    try {
      Class<? extends Name<String>> groupName = (Class<? extends Name<String>>) Class.forName(msg.getGroupname());
      commGroupHandlers.get(groupName).onNext(msg);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("GroupName not found", e);
    }
  }

  @Override
  public void register(Class<? extends Name<String>> groupName,
                       EventHandler<GroupCommMessage> commGroupNetworkHandler) {
    commGroupHandlers.put(groupName, commGroupNetworkHandler);
  }

}
