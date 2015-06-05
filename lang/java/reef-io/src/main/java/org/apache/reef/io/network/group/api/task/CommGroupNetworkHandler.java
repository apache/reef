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
package org.apache.reef.io.network.group.api.task;

import org.apache.reef.io.network.group.impl.GroupCommunicationMessage;
import org.apache.reef.io.network.group.impl.task.CommGroupNetworkHandlerImpl;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.wake.EventHandler;

/**
 * The EventHandler that receives the GroupCommunicationMsg.
 * pertaining to a specific Communication Group
 */
@DefaultImplementation(value = CommGroupNetworkHandlerImpl.class)
public interface CommGroupNetworkHandler extends EventHandler<GroupCommunicationMessage> {

  void register(Class<? extends Name<String>> operName, EventHandler<GroupCommunicationMessage> handler);

  void addTopologyElement(Class<? extends Name<String>> operName);

  GroupCommunicationMessage waitForTopologyUpdate(Class<? extends Name<String>> operName);

  byte[] waitForTopologyChanges(Class<? extends Name<String>> operName);
}
