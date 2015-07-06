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
package org.apache.reef.io.network.shuffle.ns;

import org.apache.reef.io.network.Message;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
final class ShuffleControlMessageHandlerImpl implements ShuffleControlMessageHandler {

  private final Map<String, EventHandler<Message<ShuffleControlMessage>>> eventHandlerMap;

  @Inject
  public ShuffleControlMessageHandlerImpl() {
    eventHandlerMap = new HashMap<>();
  }

  @Override
  public void onNext(final Message<ShuffleControlMessage> message) {
    final String topologyName = message.getData().iterator().next().getTopologyName();
    eventHandlerMap.get(topologyName).onNext(message);
  }

  @Override
  public void registerMessageHandler(final Class<? extends Name<String>> topologyName,
                                     final EventHandler<Message<ShuffleControlMessage>> eventHandler) {
    eventHandlerMap.put(topologyName.getName(), eventHandler);
  }
}
