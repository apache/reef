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
package com.microsoft.reef.io.network.nggroup.api;

import com.microsoft.reef.io.network.Message;
import com.microsoft.reef.io.network.nggroup.impl.GroupCommNetworkHandlerImpl;
import com.microsoft.reef.io.network.proto.ReefNetworkGroupCommProtos.GroupCommMessage;
import com.microsoft.tang.annotations.DefaultImplementation;
import com.microsoft.tang.annotations.Name;
import com.microsoft.wake.EventHandler;

/**
 *
 */
@DefaultImplementation(value = GroupCommNetworkHandlerImpl.class)
public interface GroupCommNetworkHandler extends EventHandler<Message<GroupCommMessage>> {

  /**
   * @param groupName
   * @param commGroupNetworkHandler
   */
  void register(Class<? extends Name<String>> groupName, EventHandler<GroupCommMessage> commGroupNetworkHandler);
}
